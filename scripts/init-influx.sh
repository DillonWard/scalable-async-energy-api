#!/bin/bash
set -e

echo "Waiting for InfluxDB to be ready..."
until curl -f http://influxdb:8086/ping > /dev/null 2>&1; do
    echo "InfluxDB not ready, waiting..."
    sleep 2
done

echo "InfluxDB is ready, checking setup..."

if curl -s http://influxdb:8086/api/v2/setup | grep -q '"allowed":false'; then
    echo "InfluxDB already configured"
else
    echo "Setting up InfluxDB..."
    influx setup --host http://influxdb:8086 \
        --username admin \
        --password password123 \
        --org energy_org \
        --bucket energy_metrics \
        --token energy_token \
        --force || echo "Setup completed or already exists"
fi

echo "InfluxDB initialization complete"