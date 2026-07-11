SELECT * FROM recently_played
SELECT * FROM listening_history
SELECT * FROM artist_genres
SELECT * FROM top_tracks
SELECT * FROM top_artists
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

SELECT * FROM listening_history --WITH(INDEX(listening_history_played_at_idx))
ORDER BY played_at_utc DESC

ALTER INDEX ALL ON listening_history REBUILD
update statistics listening_history with FULLSCAN

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
WHERE object_id = OBJECT_ID('recently_played')

SELECT artist_id, artist, COUNT(*) AS genre_count
FROM artist_genres
GROUP BY artist_id, artist
HAVING COUNT(*) >= 5

SELECT MIN(album_release_date) AS oldest, MAX(album_release_date) AS newest
FROM listening_history