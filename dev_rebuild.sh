#!/bin/bash
docker compose down
docker volume rm garmin-grafana-physiology_grafana_data
docker volume rm garmin-grafana-physiology_influxdb_data
git add -A
git commit -m "Introduce automated gender identifcation for TRIMP, CTL, ATL, TSB"
git push -u origin feature/add-sport-tags
docker compose up -d --build