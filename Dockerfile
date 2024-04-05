FROM python:3.11
WORKDIR /var/app
VOLUME ["/var/app"]
RUN apt-get update \
&& apt-get install -y \
   cron \
&& touch /var/app/clickhouse_event_monitor.log \
&& rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN crontab -l | \
  { cat; echo "*/10 * * * * /usr/local/bin/python3 /var/app/clickhouse_event_checker.py"; } | crontab -
CMD cron && tail -f /var/app/clickhouse_event_monitor.log
