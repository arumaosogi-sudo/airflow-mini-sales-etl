-- ============================================================================
-- Schema สำหรับ MongoDB document (sample_mflix.movies) -> PostgreSQL
--
-- หลักการแปลง NoSQL -> SQL:
--   1. field ธรรมดา            -> คอลัมน์ในตาราง movies
--   2. object ซ้อน (awards, imdb, tomatoes) -> "แบน" เป็นคอลัมน์ prefix_field
--   3. array (genres, cast, ...) -> แตกเป็นตารางลูก 1 แถวต่อ 1 ค่า + foreign key
--   4. ชนิดพิเศษของ Mongo: $oid -> TEXT,  $date/$numberLong -> DATE/TIMESTAMP
-- ============================================================================

CREATE TABLE IF NOT EXISTS movies (
    movie_id                 TEXT PRIMARY KEY,        -- _id.$oid
    title                    TEXT NOT NULL,
    type                     TEXT,
    year                     INTEGER,
    rated                    TEXT,
    runtime_min              INTEGER,                 -- runtime
    released                 DATE,                    -- released.$date.$numberLong (ms epoch)
    plot                     TEXT,
    fullplot                 TEXT,
    poster_url               TEXT,
    num_mflix_comments       INTEGER,
    lastupdated              TIMESTAMP,
    -- awards {}
    awards_wins              INTEGER,
    awards_nominations       INTEGER,
    awards_text              TEXT,
    -- imdb {}
    imdb_id                  INTEGER,
    imdb_rating              NUMERIC(3,1),
    imdb_votes               INTEGER,
    -- tomatoes {}
    tomatoes_viewer_rating   NUMERIC(3,1),
    tomatoes_viewer_reviews  INTEGER,
    tomatoes_viewer_meter    INTEGER,
    tomatoes_critic_rating   NUMERIC(3,1),
    tomatoes_critic_reviews  INTEGER,
    tomatoes_critic_meter    INTEGER,
    tomatoes_fresh           INTEGER,
    tomatoes_rotten          INTEGER,
    tomatoes_lastupdated     TIMESTAMPTZ,
    loaded_at                TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ตารางลูกสำหรับ array แต่ละตัว (many-to-one กลับไปที่ movies)
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id  TEXT NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    genre     TEXT NOT NULL,
    PRIMARY KEY (movie_id, genre)
);

CREATE TABLE IF NOT EXISTS movie_cast (                 -- "cast" เป็นคำสงวนของ SQL เลยตั้งชื่อ movie_cast
    movie_id    TEXT    NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    position    INTEGER NOT NULL,                       -- ลำดับใน array (นักแสดงนำอยู่ก่อน)
    actor_name  TEXT    NOT NULL,
    PRIMARY KEY (movie_id, position)
);

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id       TEXT    NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    position       INTEGER NOT NULL,
    director_name  TEXT    NOT NULL,
    PRIMARY KEY (movie_id, position)
);

CREATE TABLE IF NOT EXISTS movie_languages (
    movie_id  TEXT NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    language  TEXT NOT NULL,
    PRIMARY KEY (movie_id, language)
);

CREATE TABLE IF NOT EXISTS movie_countries (
    movie_id  TEXT NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    country   TEXT NOT NULL,
    PRIMARY KEY (movie_id, country)
);
