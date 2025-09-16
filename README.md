
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

## Database Tables and Descriptions :floppy_disk:

### silver.files_processed
Tracks the processing status of data files in the silver layer.

| Column       | Type                       | Description                          |
|--------------|----------------------------|------------------------------------|
| file_name    | text                       | Name or path of the processed file |
| status       | text                       | Processing status (e.g., STARTED, FINISHED, FAILED) |
| processed_at | timestamp without time zone| Timestamp when the file was processed (defaults to current timestamp) |

---

### silver.innings_deliveries
Contains ball-by-ball delivery data for each innings in matches.

| Column     | Type    | Description                           |
|------------|---------|-------------------------------------|
| match_id   | text    | Unique identifier of the match       |
| over_num   | integer | Over number in the innings            |
| ball_num   | integer | Ball number within the over           |
| inning_num | integer | Innings number (1 or 2, etc.)        |
| batter     | text    | Player batting on this delivery       |
| bowler     | text    | Player bowling on this delivery       |
| runs       | jsonb   | Runs scored on this delivery           |
| extras     | jsonb   | Extras such as wides, byes on delivery|
| wickets    | jsonb   | Wickets taken on this delivery         |
| source     | text    | Source file path for provenance        |

Indexes on `match_id` improve query performance for match-based lookups.

---

### silver.matches
Stores match-level metadata and summary information.

| Column              | Type    | Description                             |
|---------------------|---------|---------------------------------------|
| match_id            | text    | Unique match identifier (primary key) |
| event               | jsonb   | Event details such as tournament        |
| match_number        | text    | Match number or identifier              |
| dates               | date[]  | Dates on which the match was played     |
| match_type          | text    | Type of match (Test, ODI, T20, etc.)   |
| officials           | jsonb   | Match officials details                 |
| outcome             | jsonb   | Result details                         |
| overs               | integer | Number of overs (if limited overs)     |
| players             | jsonb   | List of participating players           |
| match_type_number   | text    | Numeric or string type indicator         |
| season              | text    | Season or year                          |
| team_type           | text    | Format type of competing teams          |
| toss_winner         | text    | Team that won the toss                   |
| toss_winner_decision| text    | Decision after toss (bat/field)          |
| venue               | text    | Match venue                             |
| city                | text    | City of match                           |
| player_of_match     | text[]  | List of player(s) awarded player of match|
| gender              | text    | Gender category (male/female)           |
| source              | text    | Source file path                        |

Primary key on `match_id` and indexes for fast access by match.

---

### gold.batting_stats
Aggregated batting statistics per match and player.

| Column          | Type    | Description                     |
|-----------------|---------|---------------------------------|
| match_id        | text    | Match identifier                |
| batter          | text    | Player batting                  |
| bowler          | text    | Bowler faced                   |
| num_balls_played| integer | Number of balls faced           |
| runs            | integer | Runs scored                     |
| num_boundaries  | integer | Number of boundaries (4s)       |
| num_six         | integer | Number of sixes                 |

---

### gold.bowling_stats
Aggregated bowling statistics per match and player.

| Column            | Type    | Description                    |
|-------------------|---------|-------------------------------|
| match_id          | text    | Match identifier               |
| bowler            | text    | Player bowling                 |
| num_balls_delivered| integer | Number of balls bowled         |
| runs              | integer | Runs conceded                  |
| num_boundaries    | integer | Boundaries conceded             |
| num_six           | integer | Sixes conceded                 |
| byes              | integer | Runs conceded as byes          |
| legbyes           | integer | Runs conceded as legbyes       |
| noballs           | integer | No balls bowled                |
| wides             | integer | Wides bowled                   |
| num_wickets       | integer | Wickets taken                  |

---

### gold.fielding_stats
Fielding and dismissal details per match.

| Column      | Type | Description                   |
|-------------|------|-------------------------------|
| match_id    | text | Match identifier              |
| wicket_type | text | Type of wicket (e.g., catch)  |
| fielders    | text | Names of fielders involved    |
| player_out  | text | Player dismissed               |

---

### gold.match_info
Summary and metadata at the gold layer for each match.

| Column             | Type    | Description                     |
|--------------------|---------|---------------------------------|
| match_id           | text    | Match identifier                |
| dates              | date[]  | Match dates                    |
| team_1             | text    | First competing team            |
| team_2             | text    | Second competing team           |
| team_1_players     | text[]  | Players from team 1             |
| team_2_players     | text[]  | Players from team 2             |
| event_name         | text    | Event or tournament name       |
| match_type         | text    | Type of match (Test, ODI, etc.)|
| winning_team       | text    | Winning team                   |
| won_by_wickets     | integer | Margin by wickets won          |
| won_by_runs        | integer | Margin by runs won             |
| team_type          | text    | Team type                      |
| toss_winner        | text    | Toss winner                   |
| toss_winner_decision| text    | Decision after toss (bat/field)|
| venue              | text    | Venue of the match             |
| city               | text    | City of the match             |
| gender             | text    | Gender category (male/female)  |
| player_of_match    | text[]  | List of player(s) of the match |

---

### gold.stats_processed
Tracks status of gold layer processing per match and process type.

| Column       | Type                       | Description                       |
|--------------|----------------------------|---------------------------------|
| process_name | text                       | Name of the process (e.g., batting_stats) |
| match_id     | text                       | Match identifier                |
| status       | text                       | Processing status (e.g., STARTED, FINISHED, FAILED) |
| processed_at | timestamp without time zone| Timestamp of last processed update (default is current timestamp) |

---


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
