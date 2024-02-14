#!/usr/bin/env python3
"""
    Developed by @edyatl <edyatl@yandex.ru> January 2024
    https://github.com/edyatl

"""

import os
import time
import datetime
import logging
import json
import sqlite3 as sql
import requests
import pandas as pd

import clickhouse_connect

from config import Configuration as cfg

__version__ = "0.2.0"


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
            AND event_name IN ('install', 'af_start_trial', 'af_subscribe', 'trial_renewal_cancelled')"""

        self.query_str = """SELECT event_time,event_name,af_sub1
            FROM analytics.appsflyer_export 
            WHERE media_source = 'Popunder'
            AND event_name IN ('install', 'af_start_trial', 'af_subscribe', 'trial_renewal_cancelled')
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
        Trying to connect db or creating it if not exists.
        """
        self.events_df = kwargs.get("events")
        self.install = self.events_df[self.events_df["event_name"] == "install"]
        self.trial = self.events_df[self.events_df["event_name"] == "af_start_trial"]
        self.trial_cancelled = self.events_df[
            self.events_df["event_name"] == "trial_renewal_cancelled"
        ]
        self.activation = self.events_df[self.events_df["event_name"] == "af_subscribe"]
        self.logger.debug("Make an instance of %s class", self.__class__.__name__)

        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()

            try:
                self.logger.debug("Try to connect sqlite db")
                db.execute("SELECT id FROM cachetab")
            except sql.OperationalError:
                self.logger.debug("Sqlite db not exists, creating it from schema")
                db.executescript(open(cfg.SCHEMA_FILE, "rt", encoding="utf-8").read())

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
                self.logger.info(
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

    # Functon for saving trials to cache db
    def save_trials_to_db(
        self, event_time: datetime, event_name: str, af_sub1: str
    ) -> None:
        """
        Save events to cache db.
        """
        payload = {
            "date": datetime.datetime.now(),
            "event_time": event_time,
            "event_name": event_name,
            "af_sub1": af_sub1,
        }

        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()
            db.execute("SELECT id FROM cachetab WHERE af_sub1=:af_sub1", payload)

            if len(db.fetchall()) == 0:
                db.execute(
                    "INSERT INTO cachetab (date, event_time, event_name, af_sub1)"
                    "values (:date, :event_time, :event_name, :af_sub1)",
                    payload,
                )
                try:
                    con.commit()
                    self.logger.debug(
                        "New record (%s) inserted in db", payload["af_sub1"]
                    )
                except sql.OperationalError as err:
                    self.logger.error("OOps: Operational Error: %s", err)
                    return
            else:
                self.logger.warning("Record has already in db, skipping")

    # Function for removing trials from cache db by af_sub1
    def remove_trials_from_db(self, af_sub1: str) -> None:
        """
        Remove events from cache db.
        """
        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()
            db.execute(
                "DELETE FROM cachetab WHERE af_sub1=:af_sub1", {"af_sub1": af_sub1}
            )
            try:
                con.commit()
                self.logger.debug("Record (%s) removed from db", af_sub1)
            except sql.OperationalError as err:
                self.logger.error("OOps: Operational Error: %s", err)
                return

    # Process new trials and save them to db
    def process_new_trials(self):
        """
        Process new trials and save them to db.
        """
        if self.trial.shape[0] == 0:
            return
        for index, row in self.trial.iterrows():
            self.save_trials_to_db(row["event_time"], row["event_name"], row["af_sub1"])

    # Process cancelled trials and remove them from db
    def process_cancelled_trials(self):
        """
        Process cancelled trials and remove them from db.
        """
        if self.trial_cancelled.shape[0] == 0:
            return
        for index, row in self.trial_cancelled.iterrows():
            self.remove_trials_from_db(row["af_sub1"])

    # Function gets trials from cache db where event_time <= now - 1 hour. Returns pandas DataFrame.
    def get_trials_from_db(self) -> pd.DataFrame:
        """
        Get events from cache db.
        """
        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()
            db.execute(
                """SELECT date, event_time, event_name, af_sub1 FROM cachetab
                WHERE event_name = 'af_start_trial' AND event_time <= datetime('now', '-1 hour')"""
            )
            return pd.DataFrame(
                db.fetchall(), columns=["date", "event_time", "event_name", "af_sub1"]
            )

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

    # Function confirmed_trial_requests for sending requests for trial events.
    # Gets DataFrame from get_trials_from_db then sends GET requests and then
    # deletes from db processed trials.
    def confirmed_trial_requests(self):
        """
        Send requests for trial events.
        """
        df = self.get_trials_from_db()
        if df.empty:
            return
        url = self.BASE_URL
        for af_sub1 in df["af_sub1"]:
            params = [
                ["cnv_id", af_sub1],
                ["cnv_status", "trial_started"],
                ["event2", 1],
            ]
            response, error = self.requests_call("GET", url=url, params=params)
            if error is not None:
                self.logger.error("Error: %s", error)
                continue
            self.logger.debug("Request sent: %s", response)
            self.remove_trials_from_db(af_sub1)

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
        evs.activation_requests()

        evs.process_new_trials()
        evs.process_cancelled_trials()
        evs.confirmed_trial_requests()


if __name__ == "__main__":
    main()
