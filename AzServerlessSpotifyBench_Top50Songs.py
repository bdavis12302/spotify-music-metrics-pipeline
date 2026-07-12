import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
import requests
import pandas as pd
from sqlalchemy import create_engine, text, Date
from sqlalchemy.dialects.mssql import DATETIME2
import urllib
import time
import re
import os
import sys
import logging
import argparse
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone

class RetryAfterCapture(logging.Filter):
    """Sniffs spotipy's rate-limit log line and stashes the retry-after seconds."""
    def __init__(self):
        super().__init__()
        self.retry_after = None

    def filter(self, record):
        match = re.search(r'after:\s*(\d+)', record.getMessage())
        if match:
            self.retry_after = int(match.group(1))
        return True   # never suppress, just observe

# SQL DTYPE DEFINITIONS
SQL_DTYPES = {
    'played_at_utc': DATETIME2(precision=3),
    'played_at_minute_utc': DATETIME2(precision=0),
    'played_at_hour_utc': DATETIME2(precision=0),
    'played_at_date_utc': Date(),
    'album_release_date': Date()
}

# LOGGING SETUP
LOG_PATH = r"C:\Users\blake\OneDrive\Desktop\AzServerlessSpotifyBench\pipeline.log"

file_handler = RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=3)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(funcName)s: %(message)s"
))

console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(message)s"))

# Root gets the handlers — everyone inherits
root = logging.getLogger()
root.setLevel(logging.WARNING)
root.addHandler(file_handler)
root.addHandler(console)

retry_capture = RetryAfterCapture()
file_handler.addFilter(retry_capture)

logger = logging.getLogger("AzServerlessSpotifyBench_Top50Songs")
logger.setLevel(logging.INFO)

spotipy_logger = logging.getLogger('spotipy')
spotipy_logger.setLevel(logging.WARNING)

# API PARAMETER SWITCH (ON/OFF)
parser = argparse.ArgumentParser()
parser.add_argument('--dry-run', action='store_true',
                    help='Skip all Spotify/Last.fm calls; exercise logging, SQL, and control flow only.')
args = parser.parse_args()

# Warm up the database from cold start.
def warm_up(engine, retries=5, delay=30):
    for attempt in range(retries):
        try:
            with engine.begin() as conn:
                logger.info("Database is awake!")
                return
        except Exception:
            logger.warning(f"Waking up database... attempt {attempt + 1}")
            time.sleep(delay)

# Log the date and time of a data refresh of the passed variable table.
def log_data_refresh(engine, table_name):
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'table_refresh_log')
            CREATE TABLE table_refresh_log (
                table_name NVARCHAR(100) PRIMARY KEY,
                refreshed_at_utc DATETIME2(3)
            )
        """))
        conn.execute(text("""
            MERGE table_refresh_log AS target
            USING (SELECT :name AS table_name) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET refreshed_at_utc = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (table_name, refreshed_at_utc) 
                VALUES (:name, SYSUTCDATETIME());
    """), {"name": table_name})

def create_and_push_recent_played_table(spotify, engine):
    # Pull last 50 recently played tracks
    results = spotify.current_user_recently_played(limit=50)

    tracks = []
    for item in results['items']:
        track = item['track']
        tracks.append({
        'played_at_utc': item['played_at'],
        'track_name': track['name'],
        'artist': track['artists'][0]['name'],
        'artist_id': track['artists'][0]['id'],
        'album': track['album']['name'],
        'album_id': track['album']['id'],
        'album_release_date': track['album']['release_date'],
        'album_art_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
        'track_number': track['track_number'],
        'context_type': item['context']['type'] if item['context'] else None,
        'duration_ms': track['duration_ms'],
        'popularity': track.get('popularity', None),
        'explicit': track.get('explicit', None),
        'track_id': track['id']
        })
    
    # Convert to DataFrame
    df = pd.DataFrame(tracks)
    df['duration_min'] = df['duration_ms'] / 60000
    df['played_at_utc'] = pd.to_datetime(df['played_at_utc'], utc=True)
    df['played_at_minute_utc'] = df['played_at_utc'].dt.floor('min')
    df['played_at_hour_utc'] = df['played_at_utc'].dt.floor('h')
    df['played_at_date_utc'] = df['played_at_utc'].dt.date
    df['album_release_date'] = pd.to_datetime(df['album_release_date'], errors='coerce').dt.date

    # Push to Azure SQL
    df.to_sql('recently_played', engine, if_exists='replace', index=False, dtype=SQL_DTYPES)
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE recently_played 
            ALTER COLUMN played_at_utc DATETIME2(3) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE recently_played 
            ADD CONSTRAINT PK_recently_played PRIMARY KEY (played_at_utc)
        """))
        conn.execute(text("""
            CREATE INDEX covering_recently_played_played_at_utc_idx
            ON recently_played (played_at_utc DESC)
            INCLUDE (track_name, artist, artist_id, duration_ms, duration_min, explicit)
        """))
    
    # Print Results
    logger.info(f"Successfully pushed {len(df)} tracks to Azure SQL.")

    return df

def create_history_table(df, engine):
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'listening_history'
        """))
        if result.scalar() == 0:
            empty_df = pd.DataFrame(columns=df.columns)
            empty_df.to_sql('listening_history', engine, if_exists='fail', index=False, dtype=SQL_DTYPES)
            conn.execute(text("""
                ALTER TABLE listening_history 
                ALTER COLUMN played_at_utc DATETIME2(3) NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE listening_history 
                ADD CONSTRAINT PK_listening_history PRIMARY KEY (played_at_utc)
            """))
            conn.execute(text("""
                CREATE INDEX covering_listening_history_played_at_utc_idx
                ON listening_history (played_at_utc DESC)
                INCLUDE (track_name, artist, artist_id, duration_ms, duration_min, explicit)
            """))
            logger.info(f"Listening history table created.")

def push_to_history(df, engine):
    # Push current batch to a temp staging table
    df.to_sql('staging_history', engine, if_exists='replace', index=False, dtype=SQL_DTYPES)

    with engine.begin() as conn:
        # Insert only records that don't exist in history
        conn.execute(text("""
            INSERT INTO listening_history (
                played_at_utc, track_name, artist, artist_id, album,
                album_id, album_release_date, album_art_url, track_number, context_type,
                duration_ms, duration_min, popularity, explicit, track_id,
                played_at_minute_utc, played_at_hour_utc, played_at_date_utc
            )
            SELECT 
                s.played_at_utc, s.track_name, s.artist, s.artist_id, s.album,
                s.album_id, s.album_release_date, s.album_art_url, s.track_number, s.context_type,
                s.duration_ms, s.duration_min, s.popularity, s.explicit, s.track_id,
                s.played_at_minute_utc, s.played_at_hour_utc, s.played_at_date_utc
            FROM staging_history s
            LEFT JOIN listening_history lh ON s.played_at_utc = lh.played_at_utc
            WHERE lh.played_at_utc IS NULL
        """))

        # Clean up staging table
        conn.execute(text("DROP TABLE staging_history"))

        result = conn.execute(text("SELECT COUNT(*) FROM listening_history"))
        count = result.scalar()
        logger.info(f"History table now contains {count} tracks with unique timestamps.")

def create_artist_genres_table(engine):
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'artist_genres'
        """))
        if result.scalar() == 0:
            empty_df = pd.DataFrame(columns=['artist_id', 'artist', 'genre'])
            empty_df.to_sql('artist_genres', engine, if_exists='fail', index=False)
            conn.execute(text("""
                ALTER TABLE artist_genres
                ALTER COLUMN artist_id NVARCHAR(100) NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE artist_genres
                ALTER COLUMN genre NVARCHAR(255) NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE artist_genres
                ADD CONSTRAINT PK_artist_genres PRIMARY KEY (artist_id, genre)
            """))
            conn.execute(text("""
                CREATE INDEX covering_artist_genres_artist_id_genre_idx
                ON artist_genres (artist_id, genre)
                INCLUDE (artist)
            """))
            logger.info(f"Artist genres table created.")

def push_to_artist_genres(engine, LASTFM_API_KEY):
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT artist_id, artist FROM (
                SELECT DISTINCT artist_id, artist FROM listening_history
                UNION
                SELECT DISTINCT artist_id, artist FROM top_artists
                UNION
                SELECT DISTINCT artist_id, artist FROM top_tracks
            ) all_artists
            WHERE artist_id NOT IN (SELECT artist_id FROM artist_genres)
            """))
        new_artists_df = pd.DataFrame(result.fetchall(), columns=['artist_id', 'artist'])
    
    if new_artists_df.empty:
        return

    genres = []
    for _, row in new_artists_df.iterrows():
        encoded = urllib.parse.quote(row['artist'])
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettoptags&artist={encoded}&api_key={LASTFM_API_KEY}&format=json"
        response = requests.get(url)
        data_tags = response.json()
        
        tags = data_tags.get('toptags', {}).get('tag', [])
        if tags:
            for tag in tags[:5]:
                genres.append({
                    'artist_id': row['artist_id'],
                    'artist': row['artist'],
                    'genre': tag['name']
                })
        else:
            # Insert unknown so we don't keep querying Last.fm for this artist
            genres.append({
                'artist_id': row['artist_id'],
                'artist': row['artist'],
                'genre': 'unknown'
            })
    
    if genres:
        genres_df = pd.DataFrame(genres)
        genres_df['genre'] = genres_df['genre'].str.lower()
        genres_df = genres_df.drop_duplicates(subset=['artist_id', 'genre'])
        genres_df.to_sql('artist_genres', engine, if_exists='append', index=False)
        logger.info(f"Pushed {len(genres_df)} new genre rows!")

def create_and_push_spotify_top_tracks(spotify, engine):
    top_tracks = []
    for time_range in ['short_term', 'medium_term', 'long_term']:
        for offset in [0, 50, 100, 150, 200, 250, 300, 350, 400, 450]:
            results = spotify.current_user_top_tracks(
                limit=50, offset=offset, time_range=time_range
            )
            for i, track in enumerate(results['items']):
                top_tracks.append({
                    'time_range': time_range,
                    'rank': offset + i + 1,
                    'track_name': track['name'],
                    'artist': track['artists'][0]['name'],
                    'artist_id': track['artists'][0]['id'],
                    'album': track['album']['name'],
                    'album_release_date': track['album']['release_date'],
                    'album_art_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
                    'duration_ms': track['duration_ms'],
                    'duration_min': track['duration_ms'] / 60000,
                    'explicit': track.get('explicit', None),
                    'track_id': track['id']
                })

    top_tracks_df = pd.DataFrame(top_tracks)
    top_tracks_df['album_release_date'] = pd.to_datetime(top_tracks_df['album_release_date'], errors='coerce').dt.date

    top_tracks_df.to_sql('top_tracks', engine, if_exists='replace', index=False,
                         dtype={'album_release_date': Date()})
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE top_tracks 
            ALTER COLUMN time_range NVARCHAR(100) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_tracks 
            ALTER COLUMN rank INTEGER NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_tracks 
            ALTER COLUMN artist_id NVARCHAR(255) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_tracks 
            ALTER COLUMN track_id NVARCHAR(255) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_tracks 
            ADD CONSTRAINT PK_top_tracks PRIMARY KEY (time_range, rank)
        """))
        conn.execute(text("""
            CREATE INDEX covering_top_tracks_time_range_idx
            ON top_tracks (time_range, artist_id, track_id)
            INCLUDE (rank, track_name, artist, duration_ms, duration_min, explicit)
        """))
        log_data_refresh(engine, 'top_tracks')
    
def create_and_push_spotify_top_artists(spotify, engine):
    top_artists = []
    for time_range in ['short_term', 'medium_term', 'long_term']:
        for offset in [0, 50, 100, 150]:
            results = spotify.current_user_top_artists(
                limit=50, offset=offset, time_range=time_range
            )
            for i, artist in enumerate(results['items']):
                top_artists.append({
                    'time_range': time_range,
                    'rank': offset + i + 1,
                    'artist': artist['name'],
                    'artist_id': artist['id'],
                    'artist_image_url': artist['images'][0]['url'] if artist.get('images') else None
                })

    top_artists_df = pd.DataFrame(top_artists)
    top_artists_df.to_sql('top_artists', engine, if_exists='replace', index=False)
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE top_artists 
            ALTER COLUMN time_range NVARCHAR(100) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_artists 
            ALTER COLUMN rank INTEGER NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_artists 
            ALTER COLUMN artist_id NVARCHAR(255) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE top_artists 
            ADD CONSTRAINT PK_top_artists PRIMARY KEY (time_range, rank)
        """))
        conn.execute(text("""
            CREATE INDEX covering_top_artists_time_range_idx
            ON top_artists (time_range, artist_id)
            INCLUDE (rank, artist, artist_image_url)
        """))
        log_data_refresh(engine, 'top_artists')

def create_and_push_current_playback(spotify, engine):
    current = spotify.current_playback()

    if not current or not current['item']:
        # Nothing playing — clear the snapshot so the dashboard doesn't show stale state
        with engine.begin() as conn:
            conn.execute(text("""
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'current_playback')
                DELETE FROM current_playback
            """))
        logger.info("Nothing playing right now.")
        return

    track = current['item']
    snapshot_time = pd.Timestamp.now(timezone.utc).floor('s').tz_localize(None)
    row = {
        'observed_at_utc': snapshot_time,
        'track_name': track['name'],
        'artist': track['artists'][0]['name'],
        'track_id': track['id'],
        'album': track['album']['name'],
        'album_art_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
        'device_id': current['device']['id'],
        'device_name': current['device']['name'],
        'device_type': current['device']['type'],
        'is_playing': current['is_playing'],
        'progress_ms': current['progress_ms'],
        'duration_ms': track['duration_ms'],
        'progress_pct': round(current['progress_ms'] / track['duration_ms'] * 100, 1),
        'shuffle_state': current['shuffle_state'],
        'repeat_state': current['repeat_state']
    }

    df = pd.DataFrame([row])
    df.to_sql('current_playback', engine, if_exists='replace', index=False,
              dtype={'observed_at_utc': DATETIME2(precision=0)})
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE current_playback 
            ALTER COLUMN observed_at_utc DATETIME2(3) NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE current_playback 
            ADD CONSTRAINT PK_current_playback PRIMARY KEY (observed_at_utc)
        """))
    logger.info(f"Now playing: {row['track_name']} — {row['artist']} (on {row['device_name']})")

def create_and_push_play_queue(spotify, engine):
    queue = spotify.queue()

    if not queue or not queue.get('queue'):
        with engine.begin() as conn:
            conn.execute(text("""
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'play_queue')
                DELETE FROM play_queue
            """))
        return

    rows = []
    for position, track in enumerate(queue['queue'], start=1):
        rows.append({
            'queue_position': position,
            'track_name': track['name'],
            'artist': track['artists'][0]['name'],
            'artist_id': track['artists'][0]['id'],
            'album': track['album']['name'],
            'album_art_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
            'duration_ms': track['duration_ms'],
            'track_id': track['id']
        })

    df = pd.DataFrame(rows)
    df.to_sql('play_queue', engine, if_exists='replace', index=False)

    up_next = rows[0]
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE play_queue 
            ALTER COLUMN queue_position INTEGER NOT NULL
        """))
        conn.execute(text("""
            ALTER TABLE play_queue 
            ADD CONSTRAINT PK_play_queue PRIMARY KEY (queue_position)
        """))
    logger.info(f"Up next: {up_next['track_name']} — {up_next['artist']} (+{len(rows)-1} more queued)")

def create_and_push_available_devices(spotify, engine):
    devices = spotify.devices()

    rows = []
    snapshot_time = pd.Timestamp.now(timezone.utc).floor('s').tz_localize(None)
    for d in devices['devices']:
        rows.append({
            'observed_at_utc': snapshot_time,
            'device_id': d['id'],
            'device_name': d['name'],
            'device_type': d['type'],
            'is_active': d['is_active'],
            'is_private_session': d['is_private_session'],
            'is_restricted': d['is_restricted'],
            'volume_percent': d['volume_percent'],
            'supports_volume': d['supports_volume']
        })

    if rows:
        devices_df = pd.DataFrame(rows)
        devices_df.to_sql('available_devices', engine, if_exists='replace', index=False,
                          dtype={'observed_at_utc': DATETIME2(precision=0)})
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE available_devices 
                ALTER COLUMN observed_at_utc DATETIME2(3) NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE available_devices 
                ALTER COLUMN device_name NVARCHAR(100) NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE available_devices 
                ADD CONSTRAINT PK_available_devices PRIMARY KEY (observed_at_utc, device_name)
            """))

def main():
    start = time.perf_counter()
    logger.info("=" * 50)
    logger.info("Pipeline run starting: ")
    try:
        # Spotify credentials
        SPOTIPY_CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
        SPOTIPY_CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
        SPOTIPY_REDIRECT_URI = os.environ.get('SPOTIFY_REDIRECT_URI')
        LASTFM_API_KEY = os.environ.get('LASTFM_API_KEY')

        # Azure SQL credentials
        server = os.environ.get('AZURE_SQL_SERVER')
        database = os.environ.get('AZURE_SQL_DATABASE')
        username = os.environ.get('AZURE_SQL_USERNAME')
        password = os.environ.get('AZURE_SQL_PASSWORD')

        # Connect to Azure SQL
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password}"
        )
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}",
            fast_executemany=True,
            pool_pre_ping=True,
            pool_reset_on_return='rollback'
        )
        warm_up(engine)

        # Connect to Spotify
        sp = SpotifyOAuth(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URI,
            scope="user-read-recently-played,user-read-playback-state,user-top-read"
        )
        spotify = spotipy.Spotify(
            auth_manager=sp,
            retries=0,
            status_retries=0,
            backoff_factor=0
        )

        if args.dry_run:
            logger.info("*** DRY RUN — no API calls! ***")
            df = pd.read_sql("SELECT * FROM recently_played", engine)
        else:
            df = create_and_push_recent_played_table(spotify, engine)
        
        create_history_table(df, engine)
        push_to_history(df, engine)
        create_artist_genres_table(engine)
        push_to_artist_genres(engine, LASTFM_API_KEY)

        if not args.dry_run:
            # Affinity rankings drift over weeks — weekly refresh on Mondays
            if pd.Timestamp.now(timezone.utc).floor('s').tz_localize(None).dayofweek == 0:
                create_and_push_spotify_top_tracks(spotify, engine)
                create_and_push_spotify_top_artists(spotify, engine)
            else:
                logger.info("Skipping top tracks/artists (weekly refresh on Mondays)")
            
            create_and_push_current_playback(spotify, engine)
            create_and_push_play_queue(spotify, engine)
            create_and_push_available_devices(spotify, engine)


        logger.info(f"Pipeline completed in {time.perf_counter() - start:.3f}s")

    except SpotifyException as e:
        if e.http_status == 429:
            retry_after = None
            if getattr(e, 'headers', None):
                retry_after = e.headers.get('Retry-After')
            if retry_after is None:
                retry_after = retry_capture.retry_after   # ← sniffed from spotipy's own log line
            if retry_after:
                wait = int(retry_after)
                lifts_at = datetime.now() + timedelta(seconds=wait)
                logger.warning(
                    f"Rate limited by Spotify — penalty {timedelta(seconds=wait)} "
                    f"(lifts ~{lifts_at:%Y-%m-%d %H:%M}) MT. Exiting cleanly. {str(e).replace(chr(10), ' | ')}"
                )
            else:
                logger.warning(f"Rate limited by Spotify — exiting cleanly. {e}")
            # no Task Scheduler retry, no penalty extension - Spotify Rate Limiting
            sys.exit(0)
        raise

    except Exception as e:
        logger.exception(f"PIPELINE FAILED:")
        # Signal Task Scheduler to retry
        sys.exit(1)

    finally:
        logger.info("—— end of run ——\n")

if __name__ == "__main__":
    main()