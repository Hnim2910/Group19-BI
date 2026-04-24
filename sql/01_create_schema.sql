-- =====================================================
-- BI Movie Analysis - Data Warehouse Schema
-- Star Schema with Bridge Table for Many-to-Many Genres
-- =====================================================

-- Xóa các bảng cũ nếu có
DROP TABLE IF EXISTS bridge_movie_genre CASCADE;
DROP TABLE IF EXISTS fact_movie CASCADE;
DROP TABLE IF EXISTS dim_movie CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_language CASCADE;

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- Dim Time: chiều thời gian với các cấp độ phân tích
CREATE TABLE dim_time (
    time_id       SERIAL PRIMARY KEY,
    full_date     DATE NOT NULL UNIQUE,
    year          INT NOT NULL,
    quarter       INT NOT NULL,
    month         INT NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    day           INT NOT NULL,
    day_of_week   VARCHAR(20) NOT NULL,
    decade        VARCHAR(10) NOT NULL,
    season        VARCHAR(10) NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

-- Dim Movie: thông tin phim
CREATE TABLE dim_movie (
    movie_id      SERIAL PRIMARY KEY,
    title         VARCHAR(500) NOT NULL,
    release_date  DATE,
    source_id     INT UNIQUE  -- index từ CSV gốc để tra ngược
);

-- Dim Genre: danh mục thể loại
CREATE TABLE dim_genre (
    genre_id      SERIAL PRIMARY KEY,
    genre_name    VARCHAR(50) NOT NULL UNIQUE
);

-- Dim Language: ngôn ngữ gốc
CREATE TABLE dim_language (
    language_id    SERIAL PRIMARY KEY,
    language_code  VARCHAR(10) NOT NULL UNIQUE,
    language_name  VARCHAR(50) NOT NULL
);

-- =====================================================
-- FACT TABLE
-- =====================================================

CREATE TABLE fact_movie (
    fact_id           SERIAL PRIMARY KEY,
    movie_id          INT NOT NULL REFERENCES dim_movie(movie_id),
    time_id           INT NOT NULL REFERENCES dim_time(time_id),
    language_id       INT NOT NULL REFERENCES dim_language(language_id),
    -- Measures (số đo)
    production_budget BIGINT NOT NULL,
    domestic_gross    BIGINT NOT NULL,
    foreign_gross     BIGINT NOT NULL,
    worldwide_gross   BIGINT NOT NULL,
    profit            BIGINT NOT NULL,
    profit_margin     NUMERIC(10,4),
    roi               NUMERIC(10,4),
    pct_foreign       NUMERIC(10,4),
    vote_average      NUMERIC(4,2),
    vote_count        INT,
    popularity        NUMERIC(10,3)
);

-- =====================================================
-- BRIDGE TABLE (many-to-many: movie <-> genre)
-- =====================================================

CREATE TABLE bridge_movie_genre (
    movie_id  INT NOT NULL REFERENCES dim_movie(movie_id),
    genre_id  INT NOT NULL REFERENCES dim_genre(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

-- =====================================================
-- INDEXES
-- =====================================================

CREATE INDEX idx_fact_time       ON fact_movie(time_id);
CREATE INDEX idx_fact_movie      ON fact_movie(movie_id);
CREATE INDEX idx_fact_language   ON fact_movie(language_id);
CREATE INDEX idx_bridge_movie    ON bridge_movie_genre(movie_id);
CREATE INDEX idx_bridge_genre    ON bridge_movie_genre(genre_id);
CREATE INDEX idx_time_year       ON dim_time(year);
CREATE INDEX idx_time_decade     ON dim_time(decade);

-- =====================================================
-- FIN
-- =====================================================
SELECT 'Schema created successfully!' AS status;
