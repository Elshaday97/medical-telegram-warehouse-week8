medical-telegram-warehouse/
├── .github/
│ └── workflows/
│ └── unittests.yml # CI/CD configuration [cite: 100-102]
├── api/ # Task 4: FastAPI Application [cite: 124]
│ ├── **init**.py
│ ├── main.py # FastAPI app entry point [cite: 320]
│ ├── database.py # DB connection logic [cite: 128]
│ └── schemas.py # Pydantic models [cite: 130]
├── data/ # Local data storage (Git ignored usually)
│ └── raw/ # Data Lake [cite: 161, 164]
│ ├── images/ # {channel_name}/{message_id}.jpg [cite: 161]
│ └── telegram_messages/ # YYYY-MM-DD/channel_name.json [cite: 164]
├── logs/ # Scraping logs [cite: 170]
├── medical_warehouse/ # Task 2: dbt Project [cite: 116]
│ ├── models/
│ │ ├── staging/ # Staging models (cleaning) [cite: 120]
│ │ └── marts/ # Fact/Dim tables (Star Schema) [cite: 121]
│ ├── tests/ # Custom SQL tests [cite: 122]
│ ├── dbt_project.yml # dbt configuration [cite: 117]
│ └── profiles.yml # Connection profile [cite: 118]
├── notebooks/ # Exploratory analysis [cite: 132]
├── scripts/ # Helper scripts [cite: 136]
├── src/ # Source code for scripts
│ ├── **init**.py
│ ├── scraper.py # Task 1: Scraper script [cite: 172]
│ └── yolo_detect.py # Task 3: Object detection script
├── tests/ # Unit tests for Python code [cite: 134]
├── .env # Secrets (API keys, DB creds) [cite: 103]
├── .gitignore # Ignore data/, .env, logs/ [cite: 105]
├── docker-compose.yml # Container orchestration [cite: 110]
├── Dockerfile # Python environment definition [cite: 111]
├── pipeline.py # Task 5: Dagster pipeline definition [cite: 331]
└── requirements.txt # Python dependencies [cite: 113]
