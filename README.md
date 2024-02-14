# ClickHouse Event Processor

![GitHub](https://img.shields.io/badge/license-MIT-blue.svg)

## Overview

This Python script is designed to connect to a ClickHouse Data Warehouse (DWH), retrieve information about the latest events recorded after the last program call, and execute GET requests based on the event types.

The program is divided into two main classes:

1. **ClickHouseConnector:**
   - Connects to ClickHouse DWH using specified credentials.
   - Fetches information about the latest events recorded after the last program call.
   - Utilizes a JSON file to store the previous state and track changes.

2. **EventProcessor:**
   - Processes the events obtained from ClickHouse.
   - Distributes events into different categories: Install, Trial, Activation.
   - Executes GET requests based on the event types.


**Update 2024-02-14:** when the *af_start_trial* event arrives, we wait 1 hour from *event_time* and if a new *trial_renewal_cancelled* event arrives for the same id (af_sub1), then we do nothing, and if it doesn’t arrive, then we send a get request as usual.

## Files description

```
.
├── clickhouse_event_checker.ipynb
├── clickhouse_event_checker.py
├── clickhouse_event_monitor.log
├── config.py
├── db
│   ├── cache.db
│   └── schema.sql
├── docker-compose.yml
├── Dockerfile
├── full_notebook_requirements.txt
├── requirements.txt
└── var_storage.json

```
[clickhouse_event_checker.ipynb](./clickhouse_event_checker.ipynb) - Jupyter Notebook explaining the main parts of the program.

[clickhouse_event_checker.py](./clickhouse_event_checker.py) - Main program file.

*clickhouse_event_monitor.log* - Log file with all program general events. It creates automatically when the program runs.

[config.py](./config.py) - Basic configuration. Credentials takes from environment vars.

[schema.sql](./db/schema.sql) - SQL dump of table schema. One table used at the moment.

*binom.db* - Sqlite DB file with one table. It creates automatically from schema.sql if it doesn't exist when the program runs. 

[docker-compose.yml](./docker-compose.yml) - Composer file for deployment with Docker. Sets current app host directory as container work directory.

[Dockerfile](./Dockerfile) - For deployment with Docker Python image. By default it sets hourly cron schedule task.

**Default cron table set list:**

```
# crontab -l
0 * * * * /usr/local/bin/python3 /var/app/clickhouse_event_checker.py
```

[full_notebook_requirements.txt](./full_notebook_requirements.txt) - Additional list of packages for Python3 environment for Jupyter Notebook.

[requirements.txt](./requirements.txt) - List of packages for Python3 environment to run main program file only.

*var_storage.json* - JSON file to store the previous state. It creates automatically when the program runs.

## Installation

1. Clone the repository:

    ```bash
    $ git clone https://github.com/edyatl/clickhouse-event-processor.git
    $ cd clickhouse-event-processor
    ```

2. Create an empty log file:

    ```bash
    $ touch clickhouse_event_monitor.log
    ```

3. Create credentials .env file. Next command will prompt you to insert values one by one:

    ```bash
    $ for src in $(echo 'HOST USER PASS PORT'); do \
    read -p "type ${src} value:" tkn \
    && echo "export ENV_CLICKHOUSE_${src}='${tkn}'"; done > .env
    ```

   Check resulted env file:

   ```bash
   $ cat .env
   ```


4. Set up your ClickHouse connection parameters and other configurations in the `config.py` file and in the .env file.

5. Make sure what you have docker with compose plugin properly installed:

   ```bash
   $ docker compose version
   ```

6. Run deployment with docker compose:

   ```bash
   $ docker compose up -d --build
   ```

7. Make sure what container successfully created and running:

   ```bash
   $ docker ps
   ```

   In case of empty table on command above try `docker logs` to debug.

## Usage

Run the script using the following command:

   ```bash
   $ python clickhouse_event_checker.py
   ```

## Configuration
Update the config.py file with your ClickHouse connection details, base URL for GET requests, and other configurations in the .env file.

## License
This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

