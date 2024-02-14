BEGIN TRANSACTION;
CREATE TABLE cachetab (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date       DATETIME,
    event_time DATETIME,
    event_name TEXT,
    af_sub1    TEXT
);
COMMIT;
