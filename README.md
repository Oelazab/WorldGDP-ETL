# WorldGDP-ETL

Extract, Transform, and Load global GDP data from the IMF into JSON and SQLite formats, including execution logging

## Features

- Fetches and processes the latest GDP data from the IMF.
- Saves data into `Countries_by_GDP.json` and `World_Economies.db` (SQLite database).
- Filters and displays countries with economies exceeding 100 billion USD.
- Logs all operations into `etl_log.txt`.

## Project Structure

```
WorldGDP-ETL/
├── etl_gdp.py            # Main ETL script
├── Countries_by_GDP.json # Output JSON file
├── World_Economies.db    # SQLite database
├── etl_log.txt           # Execution log
└── README.md             # Documentation
```

## Usage

Clone the repository:

```bash
git clone https://github.com/Oelazab/WorldGDP-ETL.git
cd WorldGDP-ETL
```

Run the ETL script:

```bash
python etl_gdp.py
```

Outputs:

- View Countries_by_GDP.json.
- Check the World_Economies.db database.
- Review the etl_log.txt file.
