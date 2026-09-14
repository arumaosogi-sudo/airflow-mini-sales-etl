-- Schema สำหรับข้อมูลหนังจาก MongoDB (sample_mflix.movies) แบบ normalized
-- document ซ้อนชั้น -> ตารางหลัก movies (field ซ้อนถูก flatten เป็นคอลัมน์ imdb_*, awards_*, tomatoes_*)
-- array (genres, cast, directors, languages, countries) -> ตารางลูก 1 แถวต่อ 1 ค่า

CREATE TABLE IF NOT EXISTS movies (
    mongo_id                     CHAR(24)      NOT NULL PRIMARY KEY,   -- _id.$oid จาก MongoDB
    title                        VARCHAR(255)  NOT NULL,
    year                         SMALLINT      NULL,
    type                         VARCHAR(20)   NULL,
    rated                        VARCHAR(20)   NULL,
    runtime                      SMALLINT      NULL,
    released                     DATE          NULL,                   -- จาก released.$date
    plot                         TEXT          NULL,
    fullplot                     TEXT          NULL,
    poster                       VARCHAR(500)  NULL,
    num_mflix_comments           INT           NULL,
    lastupdated                  DATETIME(6)   NULL,
    imdb_id                      INT           NULL,
    imdb_rating                  DECIMAL(3,1)  NULL,
    imdb_votes                   INT           NULL,
    awards_wins                  INT           NULL,
    awards_nominations           INT           NULL,
    awards_text                  VARCHAR(255)  NULL,
    tomatoes_viewer_rating       DECIMAL(3,1)  NULL,
    tomatoes_viewer_num_reviews  INT           NULL,
    tomatoes_viewer_meter        TINYINT       NULL,
    tomatoes_critic_rating       DECIMAL(3,1)  NULL,
    tomatoes_critic_num_reviews  INT           NULL,
    tomatoes_critic_meter        TINYINT       NULL,
    tomatoes_fresh               INT           NULL,
    tomatoes_rotten              INT           NULL,
    tomatoes_last_updated        DATETIME      NULL,
    loaded_at                    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movie_genres (
    mongo_id  CHAR(24)    NOT NULL,
    position  TINYINT     NOT NULL,
    genre     VARCHAR(50) NOT NULL,
    PRIMARY KEY (mongo_id, position),
    FOREIGN KEY (mongo_id) REFERENCES movies(mongo_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movie_cast (
    mongo_id  CHAR(24)     NOT NULL,
    position  TINYINT      NOT NULL,
    actor     VARCHAR(255) NOT NULL,
    PRIMARY KEY (mongo_id, position),
    FOREIGN KEY (mongo_id) REFERENCES movies(mongo_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movie_directors (
    mongo_id  CHAR(24)     NOT NULL,
    position  TINYINT      NOT NULL,
    director  VARCHAR(255) NOT NULL,
    PRIMARY KEY (mongo_id, position),
    FOREIGN KEY (mongo_id) REFERENCES movies(mongo_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movie_languages (
    mongo_id  CHAR(24)    NOT NULL,
    position  TINYINT     NOT NULL,
    language  VARCHAR(50) NOT NULL,
    PRIMARY KEY (mongo_id, position),
    FOREIGN KEY (mongo_id) REFERENCES movies(mongo_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS movie_countries (
    mongo_id  CHAR(24)    NOT NULL,
    position  TINYINT     NOT NULL,
    country   VARCHAR(50) NOT NULL,
    PRIMARY KEY (mongo_id, position),
    FOREIGN KEY (mongo_id) REFERENCES movies(mongo_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
