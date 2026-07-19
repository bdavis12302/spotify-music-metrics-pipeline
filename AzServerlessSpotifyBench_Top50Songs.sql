SELECT * FROM recently_played
SELECT * FROM listening_history
SELECT * FROM artist_genres
SELECT * FROM top_tracks
SELECT * FROM top_artists
select * from table_refresh_log
select * from available_devices
select * from current_playback
select * from play_queue

CREATE INDEX covering_listening_history_played_at_utc_idx
ON listening_history (played_at_utc DESC)
INCLUDE (track_name, artist, duration_ms, duration_min, explicit)

DROP INDEX covering_listening_history_played_at_utc_idx ON listening_history
sp_helpindex recently_played
sp_helpindex artist_genres
sp_helpindex listening_history
sp_helpindex top_tracks
sp_helpindex top_artists
sp_helpindex available_devices
sp_helpindex current_playback
sp_helpindex play_queue
sp_helpindex table_refresh_log

SELECT * FROM listening_history --WITH(INDEX(listening_history_played_at_idx))
ORDER BY played_at_utc DESC

ALTER INDEX ALL ON listening_history REBUILD
update statistics listening_history with FULLSCAN

ALTER INDEX ALL ON artist_genres REBUILD
update statistics artist_genres with FULLSCAN

CREATE CLUSTERED INDEX clustered_listening_history_played_at_utc_idx
ON listening_history (played_at_utc DESC)

ALTER TABLE recently_played 
    ALTER COLUMN played_at_utc DATETIME2(3) NOT NULL

ALTER TABLE recently_played 
    ADD CONSTRAINT PK_recently_played PRIMARY KEY (played_at_utc)

CREATE INDEX covering_recently_played_played_at_utc_idx
ON recently_played (played_at_utc DESC)
INCLUDE (track_name, artist, duration_ms, duration_min, explicit)

SELECT * FROM sys.indexes 
WHERE object_id = OBJECT_ID('artist_genres')

SELECT genre, TEMP.*  FROM artist_genres JOIN (
    SELECT artist_id, artist, COUNT(*) AS genre_count
    FROM artist_genres
    GROUP BY artist_id, artist
    HAVING COUNT(*) = 1) AS TEMP
    ON artist_genres.artist_id = temp.artist_id
    
SELECT MIN(album_release_date) AS oldest, MAX(album_release_date) AS newest
FROM listening_history


SELECT artist_id, artist FROM (
                SELECT DISTINCT artist_id, artist FROM listening_history
                UNION
                SELECT DISTINCT artist_id, artist FROM top_artists
                UNION
                SELECT DISTINCT artist_id, artist FROM top_tracks
            ) all_artists
            WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist_genres)

            SELECT * FROM artist_genres

SELECT artist_id, artist FROM (
            SELECT DISTINCT artist_id, artist FROM artist_genres    
            ) all_artists
            WHERE artist_id NOT IN (
            
            SELECT DISTINCT artist_id FROM listening_history
                UNION
                SELECT DISTINCT artist_id FROM top_artists
                UNION
                SELECT DISTINCT artist_id FROM top_tracks
            )