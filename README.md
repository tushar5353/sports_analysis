
![Python](https://img.shields.io/badge/Python-3572A5?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)

# Sports Analysis :sport_medal: :bar_chart: :smiley:

A modular, production-grade pipeline for extracting, transforming, and analyzing sports datasets across multiple sports disciplines.

Cricket is currently supported as a key sport with dedicated ETL pipelines and analytics modules. More sports will be added in the future to expand the ecosystem!

---

## Supported Sports Sections

### Cricket 🏏 (Source: https://cricsheet.org/downloads)

- Full ETL pipeline supporting raw data extraction, silver and gold layer transformations.
- Airflow DAG orchestration for automated workflows.
- Schema and table management, batch insertions, and match-type specific processing.
- Detailed cricket statistics: match info, batting, bowling, and fielding.

> _Future sports sections will be added here as the project grows_ 🚀

---

## Features :star2:

- **ETL Pipelines** for multiple sports data.
- **Database utilities** for scalable data ingestion and transformation.
- **Airflow orchestration** with Docker support for production workflows.
- **Configurable & extensible** for easy addition of new sports and analytics.

---

## Quickstart :zap:

### 1. Clone the Repository :arrow_down:
```
git clone https://github.com/tushar5353/sports_analysis.git
cd sports_analysis
```

### 2. Install the Package & Dependencies :package:

This project uses a `setup.py` script to manage dependencies:

`python setup.py install`

Or for development:

`pip install -e .`


### 3. Configure Your Environment :wrench:

Edit the config in `sports_analysis/utils/config.py` to set file paths, database connection details, and other environment-specific settings.

### 4. Pipeline Usage :runner:

Run ETL commands specific to a sport section:

```
python sports_analysis/cricket/etl/run.py –extract
python sports_analysis/cricket/etl/run.py –load_silver T20-ODI
python sports_analysis/cricket/etl/run.py –load_gold batting_stats
```


---

## Airflow Orchestration :cyclone: :whale:

To run the workflows with Airflow, Docker is required! :whale2:

1. Install [Docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/) :whale:.
2. From the project root directory, launch Airflow containers:
`docker-compose up`

3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080) :computer: and trigger the `cricket-ETL` DAG or other sport-specific workflows.
4. Place DAG scripts for each sport inside the `sports_analysis/<sport>/airflow` dags folder mounted in the container.

> **Note:** Configure `docker-compose.yaml` with appropriate environment variables and system resources.

---

## Project Structure :open_file_folder:

```bash
sports_analysis
├── config
│   ├── airflow.cfg
│   └── dev
│       ├── cricket.yaml
│       └── env.yaml
├── cricket
│   └── etl
│       ├── extract_raw.py
│       ├── run.py # Cricket ETL main script
│       ├── transform_to_gold.py
│       └── transform_to_silver.py
├── dags
│   └── cricket_etl.py # Cricket Airflow DAG
├── docker-compose.yaml
├── Dockerfile
├── README.md
├── setup.py
└── utils
    ├── __init__.py
    ├── config.py
    ├── database.py
    ├── logs.py
    └── utils.py
```

> _Additional sports folders and modules to be added as project evolves._

---

## Contributing :raised_hands:

- Issues and pull requests are warmly welcomed :sparkles:
- For new sports integration or bug fixes, please open an [Issue](https://github.com/tushar5353/sports_analysis/issues)

## License :page_facing_up:

MIT License. See `LICENSE` for more information.

## Author :bust_in_silhouette:

Maintained by [tushar5353](https://github.com/tushar5353)

---

Enjoy exploring sports analytics with us! :trophy: :partying_face:
