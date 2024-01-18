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

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/edyatl/clickhouse-event-processor.git
    cd clickhouse-event-processor
    ```

2. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up your ClickHouse connection parameters and other configurations in the `config.py` file and in the .env file.

## Usage

Run the script using the following command:

	```bash
	python clickhouse_event_checker.py
	```

## Configuration
Update the config.py file with your ClickHouse connection details, base URL for GET requests, and other configurations in the .env file.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

