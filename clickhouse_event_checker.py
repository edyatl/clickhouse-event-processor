#!/usr/bin/env python3
"""
    Developed by @edyatl <edyatl@yandex.ru> January 2024
    https://github.com/edyatl

"""

import os
import time
import logging
import json
import requests
import pandas as pd

import clickhouse_connect

from config import Configuration as cfg

__version__ = "0.1.1"


def get_cls_logger(cls: str) -> object:
    """
    Logger config. Sets handler to a file, formater and logging level.

    :param cls:
        str Name of class where logger calling.
    :return:
        Returns Logger instans.
    """
    logger = logging.getLogger(cls)
    if not logger.handlers:
        handler = logging.FileHandler(cfg.LOG_FILE)
        formatter = logging.Formatter(
            "%(asctime)s %(name)-16s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if cfg.DEBUG else logging.INFO)
    return logger


class ClickHouseConnector:
    """Class to connect ClickHouse DWH and fetch events."""

    logger = get_cls_logger(__qualname__)
    json_file_path = cfg.JSON_FILE

    def __init__(self, **kwargs):
        """
        Constructor func, gets credentials and makes an instance.
        """
        self.host = kwargs.get("host") or ""
        self.user = kwargs.get("user") or ""
        self.password = kwargs.get("password") or ""
        self.port = kwargs.get("port") or ""

        self.client = clickhouse_connect.get_client(
            host=self.host, user=self.user, password=self.password, port=self.port
        )

        self.query_cnt = """SELECT count()
            FROM analytics.appsflyer_export 
            WHERE media_source = 'Popunder'
            AND event_name IN ('install', 'af_start_trial', 'af_subscribe')"""

        self.query_str = """SELECT event_time,event_name,af_sub1
            FROM analytics.appsflyer_export 
            WHERE media_source = 'Popunder'
            AND event_name IN ('install', 'af_start_trial', 'af_subscribe')
            ORDER BY event_time DESC
            LIMIT {dev:int}"""

        # Check if the JSON file exists
        if os.path.exists(self.json_file_path):
            # Read the existing JSON file
            with open(self.json_file_path, "r", encoding="utf-8") as file:
                stored_values = json.load(file)
                self.prev_rows_number = stored_values.get("prev_rows_number", 0)
        else:
            self.prev_rows_number = 0
        self.logger.debug("Make an instance of %s class", self.__class__.__name__)

    def __del__(self):
        """
        Destructor func, closes connection.
        """
        self.client.close()

    def fetch_new_events(self) -> pd.DataFrame:
        """
        Fetches new events from ClickHouse DWH.
        """
        cnt = int(self.client.command(self.query_cnt))
        dev = cnt - self.prev_rows_number
        if dev == 0:
            return pd.DataFrame()
        parameters = {"dev": dev}
        result = self.client.query(self.query_str, parameters=parameters)
        with open(self.json_file_path, "w", encoding="utf-8") as file:
            json.dump({"prev_rows_number": cnt}, file)
        return pd.DataFrame(result.result_rows, columns=result.column_names)


class EventProcessor:
    """Class for processing events."""

    BASE_URL = cfg.BASE_URL
    logger = get_cls_logger(__qualname__)

    def __init__(self, **kwargs):
        """
        Constructor func, gets events DataFrame and makes an instance.
        """
        self.events_df = kwargs.get("events")
        self.install = self.events_df[self.events_df["event_name"] == "install"]
        self.trial = self.events_df[self.events_df["event_name"] == "af_start_trial"]
        self.activation = self.events_df[self.events_df["event_name"] == "af_subscribe"]
        self.logger.debug("Make an instance of %s class", self.__class__.__name__)

    def requests_call(self, verb: str, url: str, params=None, **kwargs) -> tuple:
        """
        Wraping func for requests with errors handling.

        :param verb:
            str Method of request ``get`` or ``post``.
        :param url:
            str URL to connect.
        :return:
            Returns a tuple of response object and error.
            If an error occurs, the response will be empty
            and vice versa otherwise.
        """
        r: object = None
        error: str = None
        retries: int = cfg.RETRIES  # default 10
        delay: int = cfg.DELAY  # default 6

        for retry in range(retries):
            try:
                self.logger.debug("Try %s request %s", verb, url)
                r = requests.request(verb, url, params=params)
                r.raise_for_status()
                self.logger.debug(
                    "Get answer with status code: %s %s", r.status_code, r.reason
                )
                return r, error
            except requests.exceptions.HTTPError as errh:
                self.logger.error("Http Error: %s", errh)
                error = errh
                self.logger.debug(
                    "Don't give up! Trying to reconnect, retry %s of %s",
                    retry + 1,
                    retries,
                )
                time.sleep(delay)
            except requests.exceptions.ConnectionError as errc:
                self.logger.error("Connection Error: %s", errc)
                error = errc
                self.logger.debug(
                    "Don't give up! Trying to reconnect, retry %s of %s",
                    retry + 1,
                    retries,
                )
                time.sleep(delay)
            except requests.exceptions.Timeout as errt:
                self.logger.error("Timeout Error: %s", errt)
                error = errt
                self.logger.debug(
                    "Don't give up! Trying to reconnect, retry %s of %s",
                    retry + 1,
                    retries,
                )
                time.sleep(delay)
            except requests.exceptions.RequestException as err:
                self.logger.error("OOps: Unexpected Error: %s", err)
                error = err
                self.logger.debug(
                    "Don't give up! Trying to reconnect, retry %s of %s",
                    retry + 1,
                    retries,
                )
                time.sleep(delay)

        return r, error

    def install_requests(self):
        """
        Send requests for install events.
        """
        if self.install.shape[0] == 0:
            return
        url = self.BASE_URL
        for af_sub1 in self.install["af_sub1"]:
            params = [
                ["cnv_id", af_sub1],
                ["cnv_status", "install"],
                ["event1", 1],
            ]
            response, error = self.requests_call("GET", url=url, params=params)

    def trial_requests(self):
        """
        Send requests for trial events.
        """
        if self.trial.shape[0] == 0:
            return
        url = self.BASE_URL
        for af_sub1 in self.trial["af_sub1"]:
            params = [
                ["cnv_id", af_sub1],
                ["cnv_status", "trial_started"],
                ["event2", 1],
            ]
            response, error = self.requests_call("GET", url=url, params=params)

    def activation_requests(self):
        """
        Send requests for activation events.
        """
        if self.activation.shape[0] == 0:
            return
        url = self.BASE_URL
        for af_sub1 in self.activation["af_sub1"]:
            params = [
                ["cnv_id", af_sub1],
                ["cnv_status", "trial_converted"],
                ["event4", 1],
            ]
            response, error = self.requests_call("GET", url=url, params=params)


def main():
    dwh = ClickHouseConnector(
        host=cfg.CLICKHOUSE_HOST,
        user=cfg.CLICKHOUSE_USER,
        password=cfg.CLICKHOUSE_PASS,
        port=cfg.CLICKHOUSE_PORT,
    )

    df = dwh.fetch_new_events()

    del dwh

    if not df.empty:
        evs = EventProcessor(events=df)
        evs.install_requests()
        evs.trial_requests()
        evs.activation_requests()


if __name__ == "__main__":
    main()
