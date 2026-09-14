-- ทุกคอลัมน์ต้องเป็น 1 (TRUE) ไม่งั้น SQLCheckOperator จะ fail
SELECT
    (SELECT COUNT(*) FROM movies) > 0                                              AS has_movies,
    (SELECT COUNT(*) FROM movies WHERE title IS NULL OR title = '') = 0            AS no_empty_title,
    (SELECT COUNT(*) FROM movies WHERE imdb_rating < 0 OR imdb_rating > 10) = 0    AS imdb_rating_in_range,
    (SELECT COUNT(*) FROM movies m
       WHERE NOT EXISTS (SELECT 1 FROM movie_genres g WHERE g.mongo_id = m.mongo_id)) = 0
                                                                                   AS every_movie_has_genre,
    (SELECT COUNT(*) FROM movie_cast c
       LEFT JOIN movies m ON m.mongo_id = c.mongo_id WHERE m.mongo_id IS NULL) = 0 AS no_orphan_cast;
