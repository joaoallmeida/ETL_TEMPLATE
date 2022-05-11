USE monitoring;
CREATE TABLE IF NOT EXISTS etl_process (

    process_id INT AUTO_INCREMENT PRIMARY KEY,
    process TEXT,
    created_by TEXT,
    created_at DATETIME,
    updated_at DATETIME

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ;

CREATE TABLE IF NOT EXISTS etl_logging (

    log_id INT AUTO_INCREMENT PRIMARY KEY,
    process_id INT NOT NULL,
    table_name TEXT,
    start_date DATETIME NOT NULL,
    complete_date DATETIME ,
    row_count INT DEFAULT 0,
    status TEXT NOT NULL,
    error_message TEXT,
    
    CONSTRAINT fk_process FOREIGN KEY (process_id) REFERENCES etl_process(process_id)
    
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE bronze;
CREATE TABLE IF NOT EXISTS yts_movies (
  movie_sk bigint AUTO_INCREMENT PRIMARY KEY,
  id bigint ,
  url text,
  imdb_code text,
  title text,
  title_english text,
  title_long text,
  slug text,
  year bigint ,
  rating double ,
  runtime bigint ,
  summary text,
  description_full text,
  synopsis text,
  yt_trailer_code text,
  language text,
  mpa_rating text,
  background_image text,
  background_image_original text,
  small_cover_image text,
  medium_cover_image text,
  large_cover_image text,
  state text,
  date_uploaded timestamp,
  date_uploaded_unix bigint ,
  genre_0 text,
  genre_1 text,
  genre_2 text,
  genre_3 text,
  genre_4 text,
  url_tt text,
  hash text,
  quality text,
  type text,
  seeds bigint ,
  peers bigint ,
  size text,
  size_bytes bigint ,
  date_uploaded_tt timestamp,
  date_uploaded_unix_tt bigint ,
  url_torrent text,
  extracting_at timestamp NULL ,
  extracting_by text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- db_movies_silver.yts_movies definition
USE silver;
CREATE TABLE IF NOT EXISTS yts_movies (
  movie_sk bigint AUTO_INCREMENT PRIMARY KEY,
  id bigint ,
  url_yts text,
  imdb_code text,
  title text,
  year bigint ,
  rating double ,
  runtime bigint ,
  summary text,
  yt_trailer_code text,
  language text,
  banner_image text,
  uploaded_content_at datetime ,
  genre_0 text,
  genre_1 text,
  genre_2 text,
  genre_3 text,
  genre_4 text,
  quality text,
  type text,
  size text,
  size_bytes bigint ,
  uploaded_torrent_at datetime ,
  url_torrent text,
  extracting_at datetime ,
  extracting_by text,
  loaded_at timestamp NULL ,
  loaded_by text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;