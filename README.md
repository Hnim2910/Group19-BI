# Group19-BI

Business Intelligence project for movie revenue, genre, time trend, and rating analysis.

## Project Description

This project builds a Business Intelligence system for analyzing movie market performance from a cleaned movie revenue dataset. The system focuses on turning raw movie data into a structured Data Warehouse and an interactive Power BI dashboard that supports financial, genre, time-based, and rating analysis.

The original dataset contains movie records from 1946 to 2019. During data profiling, the data before 2010 was found to be sparse and uneven, while 2019 contained too few records for reliable trend analysis. Therefore, the analytical scope was restricted to the 2010-2018 period. After cleaning and deduplication, the final analytical dataset contains 1,541 unique movie records.

The project includes:

- An ETL pipeline written in Python using pandas and SQLAlchemy.
- A PostgreSQL Data Warehouse designed with an enhanced Star Schema.
- A bridge table for handling the many-to-many relationship between movies and genres.
- A Power BI dashboard for interactive business analysis.
- A final report documenting the dataset, warehouse design, ETL process, dashboard, and insights.

Key analysis areas:

- Worldwide, domestic, and foreign revenue performance.
- Production budget, profit, profit margin, and ROI.
- Revenue and movie count trends from 2010 to 2018.
- Genre-level performance and capital efficiency.
- Rating distribution and top-performing movies.
- Seasonal and monthly release patterns.

The final dashboard provides four main pages:

- **Overview**: high-level KPIs, yearly revenue trend, profit by year, top movies, and revenue by genre.
- **Genre Analysis**: movie count, revenue, ROI, and rating by genre.
- **Time Trends**: monthly revenue, yearly movie count, ROI by year, and domestic/foreign revenue split.
- **Rating & Top Performers**: rating distribution, blockbuster/flop indicators, budget-vs-revenue scatter plot, and top movie table.

## Project Structure

- `dashboard/`: Power BI PBIP report and semantic model.
- `data/raw/final_dataset.csv`: source dataset used by the ETL pipeline.
- `docs/GROUP 19 BI FINAL REPORT.docx`: final report document.
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
4. Run the sql script in sql/01_create_schema.sql
5. Copy `.env.example` to `.env` and update local database credentials.
6. Run the ETL pipeline to load the data in to the database:

```powershell
python etl/etl_pipeline.py
```

7. Open `dashboard/movie_dashboard.pbip` in Power BI Desktop.

## Dataset Credit

The dataset used in this project is credited to the GitHub repository
[ntdoris/movie-revenue-analysis](https://github.com/ntdoris/movie-revenue-analysis).
