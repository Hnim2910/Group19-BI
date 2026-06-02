# Group19-BI

Business Intelligence project for movie revenue, genre, time trend, and rating analysis.

## Project Structure

- `dashboard/`: Power BI PBIP report and semantic model.
- `data/raw/final_dataset.csv`: source dataset used by the ETL pipeline.
- `etl/etl_pipeline.py`: ETL script for loading/transformation.
- `sql/01_create_schema.sql`: database schema script.
- `.env.example`: sample environment configuration.

## Setup

1. Clone this repository
2. Create a Python virtual environment.
3. Install dependencies:

```powershell
pip install -r requirements.txt
```

4. Copy `.env.example` to `.env` and update local database credentials.
5. Run the ETL pipeline if the database needs to be populated:

```powershell
python etl/etl_pipeline.py
```

6. Open `dashboard/movie_dashboard.pbip` in Power BI Desktop.

## Dataset Credit

The dataset used in this project is credited to the GitHub repository
[ntdoris/movie-revenue-analysis](https://github.com/ntdoris/movie-revenue-analysis).
