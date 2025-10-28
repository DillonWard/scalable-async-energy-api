#!/bin/bash
set -e


python3 -c "
import time
import psycopg2
import os

database_url = os.getenv('DATABASE_URL', 'postgresql://energy_user:energy_pass@postgres:5432/energy_db')
host = 'postgres'
user = 'energy_user'
password = 'energy_pass'
database = 'energy_db'

for i in range(15):
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, database=database)
        conn.close()
        print('Database is ready!')
        break
    except psycopg2.OperationalError:
        print(f'Database not ready, waiting... ({i+1}/15)')
        time.sleep(2)
else:
    print('Database failed to become ready')
    exit(1)
"
python3 /app/scripts/import_data.py

exec "$@"