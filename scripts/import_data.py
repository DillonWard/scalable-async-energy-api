import os
import sys
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_database_connection(database_url: str, max_retries: int = 30) -> bool:
    for attempt in range(max_retries):
        try:
            engine = create_engine(database_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.info(
                f"Database connection attempt {attempt + 1}/{max_retries} failed: {e}"
            )
            time.sleep(2)

    logger.error("Failed to connect to database after all retries")
    return False


def is_database_empty(database_url: str) -> bool:
    try:
        engine = create_engine(database_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM device_readings"))
            count = result.scalar()
            logger.info(f"Found {count} records in device_readings table")
            return count == 0
    except Exception as e:
        logger.error(f"Error checking database: {e}")
        return False


def extract_data_file(data_dir: str) -> str:
    zip_path = os.path.join(data_dir, "data.zip")
    csv_path = os.path.join(data_dir, "smart_home_energy_consumption_large.csv")

    if os.path.exists(csv_path):
        logger.info("CSV file already extracted")
        return csv_path

    if os.path.exists(zip_path):
        logger.info("Extracting data from zip file...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_dir)

        extracted_csv = os.path.join(
            data_dir, "data", "smart_home_energy_consumption_large.csv"
        )
        if os.path.exists(extracted_csv):
            os.rename(extracted_csv, csv_path)
            os.rmdir(os.path.join(data_dir, "data"))
            logger.info("Data extracted successfully")
            return csv_path

    raise FileNotFoundError("No data.zip file found")


def import_csv_data(csv_path: str, database_url: str, batch_size: int = 10000):
    logger.info(f"Starting data import from {csv_path}")

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded CSV with {len(df)} rows and columns: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return False

    try:
        if "date" in df.columns and "time" in df.columns:
            df["full_timestamp"] = pd.to_datetime(
                df["date"].astype(str) + " " + df["time"].astype(str),
                format="%Y-%m-%d %H:%M",
            )
        elif "date" in df.columns:
            df["full_timestamp"] = pd.to_datetime(df["date"])
        else:
            df["full_timestamp"] = pd.to_datetime("now")

        clean_df = pd.DataFrame()

        clean_df["home_id"] = df["home_id"].astype(str)
        clean_df["appliance_type"] = df["appliance_type"].astype(str)
        clean_df["energy_consumption"] = pd.to_numeric(
            df["energy_consumption"], errors="coerce"
        )
        clean_df["timestamp"] = df["full_timestamp"]
        clean_df["date"] = df["full_timestamp"].dt.date
        clean_df["outdoor_temperature"] = pd.to_numeric(
            df["outdoor_temperature"], errors="coerce"
        )
        clean_df["season"] = df["season"].astype(str)
        clean_df["household_size"] = pd.to_numeric(
            df["household_size"], errors="coerce"
        )

    except Exception as e:
        logger.error(f"Error processing CSV columns: {e}")
        logger.info(f"Available columns: {list(df.columns)}")
        return False

    initial_count = len(clean_df)
    clean_df = clean_df.dropna(subset=["home_id", "energy_consumption"])
    logger.info(f"Cleaned data: {len(clean_df)} valid rows from {initial_count} total")

    engine = create_engine(database_url)

    try:
        total_imported = 0
        for i in range(0, len(clean_df), batch_size):
            batch = clean_df.iloc[i : i + batch_size]
            batch.to_sql(
                "device_readings",
                engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total_imported += len(batch)
            logger.info(f"Imported batch: {total_imported}/{len(clean_df)} records")

        logger.info(f"Successfully imported {total_imported} records")
        return True

    except Exception as e:
        logger.error(f"Error importing data: {e}")
        return False


def main():
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://energy_user:energy_pass@postgres:5432/energy_db"
    )
    data_dir = "/app/data"

    logger.info("Starting data import process...")

    if not check_database_connection(database_url):
        logger.error("Cannot connect to database, exiting")
        sys.exit(1)

    if not is_database_empty(database_url):
        logger.info("Database already contains data, skipping import")
        return True

    logger.info("Database is empty, proceeding with data import...")

    try:
        csv_path = extract_data_file(data_dir)

        success = import_csv_data(csv_path, database_url)

        if success:
            logger.info("Data import completed successfully")
            return True
        else:
            logger.error("Data import failed")
            return False

    except Exception as e:
        logger.error(f"Unexpected error during import: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
