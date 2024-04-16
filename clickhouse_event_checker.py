#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Developed by @edyatl <edyatl@yandex.ru> January 2024
    https://github.com/edyatl

"""

import os
import time
from datetime import datetime, timedelta
import json
import sqlite3 as sql
import requests
import pandas as pd

import clickhouse_connect

from config import Configuration as cfg
from logger import get_cls_logger

__version__ = "0.3.0"


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

        self.query_str = """SELECT created, event_time, event_name, af_sub1
            FROM analytics.appsflyer_export FINAL
            WHERE media_source = 'Popunder' 
                AND created > {prev_last_created:datetime}
                AND event_name IN ('install', 'af_start_trial', 'af_subscribe', 'trial_renewal_cancelled')
            ORDER BY event_time DESC"""

        # Check if the JSON file exists
        if os.path.exists(self.json_file_path):
            # Read the existing JSON file
            with open(self.json_file_path, "r", encoding="utf-8") as file:
                stored_values = json.load(file)
                self.prev_last_created = datetime.fromisoformat(
                    stored_values.get("prev_last_created", 0)
                )
        else:
            self.prev_last_created = datetime.now() - timedelta(weeks=1)
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
        parameters = {"prev_last_created": self.prev_last_created}
        result = self.client.query(self.query_str, parameters=parameters)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        if df.empty:
            return pd.DataFrame()
        with open(self.json_file_path, "w", encoding="utf-8") as file:
            json.dump({"prev_last_created": str(df["created"].max())}, file)
        return df


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

        self._create_db_if_not_exists()

    def _create_db_if_not_exists(self):
        """Create SQLite database if not exists."""
        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()
            try:
                self.logger.debug("Try to connect sqlite db")
                db.execute("SELECT id FROM cachetab")
            except sql.OperationalError:
                self.logger.debug("Sqlite db not exists, creating it from schema")
                db.executescript(open(cfg.SCHEMA_FILE, "rt", encoding="utf-8").read())

    def _save_event_to_db(
        self, event_time: datetime, event_name: str, af_sub1: str
    ) -> bool:
        """
        Save event to cache db.
        """
        payload = {
            "date": datetime.now(),
            "event_time": event_time,
            "event_name": event_name,
            "af_sub1": af_sub1,
        }

        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()
            db.execute(
                "SELECT id FROM cachetab WHERE af_sub1=:af_sub1 AND event_name=:event_name",
                payload,
            )

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
                    return True
                except sql.OperationalError as err:
                    self.logger.error("OOps: Operational Error: %s", err)
                    return False
            else:
                self.logger.warning("Record has already in db, skipping")
                return False

    def remove_event_from_db(self, af_sub1: str) -> None:
        """
        Remove event from cache db.
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

    def _process_new_events(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and save new events to cache db.
        """
        new_events_df = pd.DataFrame(columns=events_df.columns)
        for index, row in events_df.iterrows():
            if self._save_event_to_db(
                row["event_time"], row["event_name"], row["af_sub1"]
            ):
                new_events_df.loc[len(new_events_df)] = row
        return new_events_df

    def remove_old_events(self):
        """
        Remove old events from cache db.
        """
        week_ago = datetime.now() - timedelta(weeks=1)
        with sql.connect(cfg.DB_FILE, timeout=10) as con:
            db = con.cursor()
            db.execute(
                "DELETE FROM cachetab WHERE event_time < :week_ago",
                {"week_ago": week_ago},
            )
            try:
                con.commit()
                self.logger.debug("Old records removed from db")
            except sql.OperationalError as err:
                self.logger.error("OOps: Operational Error: %s", err)
                return

    def process_install_events(self):
        """
        Process install events.
        """
        self.install = self._process_new_events(self.install)
        self.install_requests()

    def process_trial_events(self):
        """
        Process trial events.
        """
        self.trial = self._process_new_events(self.trial)
        self.trial_requests()

    def process_cancelled_trial_events(self):
        """
        Process cancelled trial events.
        """
        self.trial_cancelled = self._process_new_events(self.trial_cancelled)
        self.cancel_trial_requests()

    def process_activation_events(self):
        """
        Process activation events.
        """
        self.activation = self._process_new_events(self.activation)
        self.activation_requests()

    def install_requests(self):
        """
        Send requests for install events.
        """
        self.send_event_requests("install", "install", 1)

    def activation_requests(self):
        """
        Send requests for activation events.
        """
        self.send_event_requests("activation", "trial_converted", 4)

    def trial_requests(self):
        """
        Send requests for trial events.
        """
        self.send_event_requests("trial", "trial_started", 2)

    def cancel_trial_requests(self):
        """
        Send requests for cancelled trial events.
        """
        self.send_event_requests("trial_cancelled", "trial_renewal_cancelled", 6)

    def send_event_requests(self, event_name, event_status, event_number):
        """
        Send requests for specified event.
        """
        events_df = getattr(self, event_name)
        if events_df.shape[0] == 0:
            return
        url = self.BASE_URL
        for af_sub1 in events_df["af_sub1"]:
            params = [
                ["cnv_id", af_sub1],
                ["cnv_status", event_status],
                [f"event{event_number}", 1],
            ]
            response, error = self._requests_call("GET", url=url, params=params)
            if error is not None:
                self.logger.error(
                    "Error while transmiting event (%s): %s", af_sub1, error
                )
                continue

    def _requests_call(self, verb: str, url: str, params=None, **kwargs) -> tuple:
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


def main():
    """
    Main function.
    """
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

        evs.process_install_events()
        evs.process_activation_events()
        evs.process_trial_events()
        evs.process_cancelled_trial_events()

        evs.remove_old_events()


if __name__ == "__main__":
    main()
