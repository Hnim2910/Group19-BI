"""
===================================================================
BI Movie Analysis - ETL Pipeline
===================================================================
Extract : Đọc final_dataset.csv
Transform: Làm sạch, chuẩn hóa, tách dimensions
Load    : Đưa vào PostgreSQL theo Star Schema

Chạy: python etl_pipeline.py
===================================================================
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ===== Load config từ file .env =====
load_dotenv()

DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "movie_dw")

CSV_PATH = "data/raw/final_dataset.csv"

# Map mã ngôn ngữ ISO 639-1 sang tên đầy đủ
LANGUAGE_MAP = {
    "en": "English", "fr": "French", "de": "German", "es": "Spanish",
    "it": "Italian", "ja": "Japanese", "ko": "Korean", "zh": "Chinese",
    "pt": "Portuguese", "ru": "Russian", "hi": "Hindi", "nl": "Dutch",
    "sv": "Swedish", "da": "Danish", "cn": "Chinese (Cantonese)",
}


# ===================================================================
# 1. EXTRACT
# ===================================================================
def extract(csv_path: str) -> pd.DataFrame:
    print(f"[EXTRACT] Đọc dữ liệu từ {csv_path}")
    df = pd.read_csv(csv_path, index_col=0)
    print(f"          -> {len(df)} dòng, {df.shape[1]} cột")
    return df


# ===================================================================
# 2. TRANSFORM
# ===================================================================
def transform(df: pd.DataFrame) -> dict:
    print("[TRANSFORM] Bắt đầu xử lý dữ liệu...")

    # Xóa dòng thiếu genres (8 dòng)
    df = df.dropna(subset=["genres"]).copy()
    df = df.reset_index(drop=True)
    df["source_id"] = df.index  # giữ ID gốc

    # --- Chuẩn hóa release_date ---
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    # Dòng nào không parse được thì dùng year + month của CSV
    mask = df["release_date"].isna()
    df.loc[mask, "release_date"] = pd.to_datetime(
        df.loc[mask, "year"].astype(str) + "-"
        + df.loc[mask, "month"].astype(str) + "-01",
        errors="coerce"
    )
    df = df.dropna(subset=["release_date"]).reset_index(drop=True)

    # --- DIM TIME ---
    print("            - Xây dim_time")
    unique_dates = pd.Series(df["release_date"].unique()).sort_values()
    dim_time = pd.DataFrame({"full_date": unique_dates})
    dim_time["year"]        = dim_time["full_date"].dt.year
    dim_time["quarter"]     = dim_time["full_date"].dt.quarter
    dim_time["month"]       = dim_time["full_date"].dt.month
    dim_time["month_name"]  = dim_time["full_date"].dt.strftime("%B")
    dim_time["day"]         = dim_time["full_date"].dt.day
    dim_time["day_of_week"] = dim_time["full_date"].dt.strftime("%A")
    dim_time["decade"]      = (dim_time["year"] // 10 * 10).astype(str) + "s"
    dim_time["season"]      = dim_time["month"].apply(_get_season)
    dim_time["is_weekend"]  = dim_time["full_date"].dt.dayofweek >= 5
    dim_time = dim_time.reset_index(drop=True)
    dim_time["time_id"] = dim_time.index + 1

    # --- DIM MOVIE ---
    print("            - Xây dim_movie")
    dim_movie = df[["movie", "release_date", "source_id"]].copy()
    dim_movie.columns = ["title", "release_date", "source_id"]
    dim_movie = dim_movie.reset_index(drop=True)
    dim_movie["movie_id"] = dim_movie.index + 1

    # --- DIM GENRE ---
    print("            - Xây dim_genre")
    all_genres = set()
    for g in df["genres"]:
        all_genres.update(s.strip() for s in str(g).split(","))
    dim_genre = pd.DataFrame({"genre_name": sorted(all_genres)})
    dim_genre["genre_id"] = dim_genre.index + 1

    # --- DIM LANGUAGE ---
    print("            - Xây dim_language")
    unique_langs = df["original_language"].unique()
    dim_language = pd.DataFrame({"language_code": sorted(unique_langs)})
    dim_language["language_name"] = dim_language["language_code"].map(
        LANGUAGE_MAP
    ).fillna(dim_language["language_code"].str.upper())
    dim_language["language_id"] = dim_language.index + 1

    # --- BRIDGE MOVIE-GENRE ---
    print("            - Xây bridge_movie_genre")
    bridge_rows = []
    genre_to_id = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))
    for _, row in df.iterrows():
        movie_id = dim_movie.loc[dim_movie["source_id"] == row["source_id"],
                                 "movie_id"].iloc[0]
        for g in str(row["genres"]).split(","):
            g = g.strip()
            if g in genre_to_id:
                bridge_rows.append({
                    "movie_id": movie_id,
                    "genre_id": genre_to_id[g]
                })
    bridge = pd.DataFrame(bridge_rows).drop_duplicates()

    # --- FACT MOVIE ---
    print("            - Xây fact_movie")
    date_to_id = dict(zip(dim_time["full_date"], dim_time["time_id"]))
    lang_to_id = dict(zip(dim_language["language_code"],
                          dim_language["language_id"]))
    movie_map  = dict(zip(dim_movie["source_id"], dim_movie["movie_id"]))

    fact = pd.DataFrame({
        "movie_id":          df["source_id"].map(movie_map),
        "time_id":           df["release_date"].map(date_to_id),
        "language_id":       df["original_language"].map(lang_to_id),
        "production_budget": df["production_budget"],
        "domestic_gross":    df["domestic_gross"],
        "foreign_gross":     df["foreign_gross"],
        "worldwide_gross":   df["worldwide_gross"],
        "profit":            df["profit"],
        "profit_margin":     df["profit_margin"],
        "roi":               df["roi"],
        "pct_foreign":       df["pct_foreign"],
        "vote_average":      df["vote_average"],
        "vote_count":        df["vote_count"],
        "popularity":        df["popularity"],
    })

    # Xử lý giá trị vô cực (inf, -inf) và NaN
    # Các cột tỷ lệ có thể bị chia cho 0 -> inf, PostgreSQL NUMERIC không chấp nhận
    import numpy as np
    numeric_cols = ["profit_margin", "roi", "pct_foreign",
                    "vote_average", "popularity"]
    for col in numeric_cols:
        fact[col] = fact[col].replace([np.inf, -np.inf], np.nan)
    n_fixed = fact[numeric_cols].isna().sum().sum()
    if n_fixed > 0:
        print(f"            - Đã chuyển {n_fixed} giá trị inf/NaN thành NULL")

    print(f"[TRANSFORM] Xong. Tổng quan:")
    print(f"            dim_time     : {len(dim_time):>6} dòng")
    print(f"            dim_movie    : {len(dim_movie):>6} dòng")
    print(f"            dim_genre    : {len(dim_genre):>6} dòng")
    print(f"            dim_language : {len(dim_language):>6} dòng")
    print(f"            bridge       : {len(bridge):>6} dòng")
    print(f"            fact_movie   : {len(fact):>6} dòng")

    return {
        "dim_time":     dim_time,
        "dim_movie":    dim_movie,
        "dim_genre":    dim_genre,
        "dim_language": dim_language,
        "bridge":       bridge,
        "fact":         fact,
    }


def _get_season(month: int) -> str:
    """Theo Bắc bán cầu"""
    if month in (12, 1, 2):   return "Winter"
    if month in (3, 4, 5):    return "Spring"
    if month in (6, 7, 8):    return "Summer"
    return "Fall"


# ===================================================================
# 3. LOAD
# ===================================================================
def load(tables: dict):
    conn_str = (f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
                f"@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    engine = create_engine(conn_str)
    print(f"[LOAD] Kết nối tới {DB_HOST}:{DB_PORT}/{DB_NAME}")

    with engine.begin() as conn:
        # Truncate trước khi load lại (TRUNCATE giữ được schema)
        conn.execute(text("""
            TRUNCATE TABLE
                bridge_movie_genre,
                fact_movie,
                dim_movie,
                dim_time,
                dim_genre,
                dim_language
            RESTART IDENTITY CASCADE;
        """))

        load_order = [
            ("dim_time",     tables["dim_time"]),
            ("dim_movie",    tables["dim_movie"]),
            ("dim_genre",    tables["dim_genre"]),
            ("dim_language", tables["dim_language"]),
            ("fact_movie",   tables["fact"]),
            ("bridge_movie_genre", tables["bridge"]),
        ]

        for name, df_tbl in load_order:
            df_tbl.to_sql(name, conn, if_exists="append", index=False)
            print(f"       ✓ Load {name}: {len(df_tbl)} dòng")

    print("[LOAD] Hoàn tất!")


# ===================================================================
# MAIN
# ===================================================================
if __name__ == "__main__":
    raw = extract(CSV_PATH)
    tables = transform(raw)
    load(tables)
    print("\n🎉 ETL pipeline chạy thành công!")
