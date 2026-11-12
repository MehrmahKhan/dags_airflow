"""
Daily currency exchange rate ETL.

Pulls rates from the Frankfurter public API, reshapes every rate into one
flat document, and writes the documents into MongoDB.

The three tasks hand data to each other using named XComs:

    extract_currency_rates   pushes key "raw_rates"
    transform_exchange_rates pulls "raw_rates", pushes key "transformed_rates"
    load_rates_into_mongodb  pulls "transformed_rates"
"""

from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]

MONGO_URI = "mongodb://mongo_user:mongo_pass@mongodb:27017/?authSource=admin"
MONGO_DB_NAME = "currency_db"
MONGO_COLLECTION_NAME = "exchange_rates"

SOURCE_NAME = "Frankfurter API"


# ----------------------------------------------------------------------
# Task 1: Extract
# ----------------------------------------------------------------------

def extract_currency_rates(**context):
    """Call the Frankfurter API and push the untouched response to XCom."""

    quotes_value = ""
    for index in range(len(TARGET_CURRENCIES)):
        if index > 0:
            quotes_value = quotes_value + ","
        quotes_value = quotes_value + TARGET_CURRENCIES[index]

    request_params = {
        "base": BASE_CURRENCY,
        "quotes": quotes_value,
    }

    print("Calling " + API_URL + " with base " + BASE_CURRENCY + " and quotes " + quotes_value)

    response = requests.get(API_URL, params=request_params, timeout=30)
    response.raise_for_status()

    raw_payload = response.json()
    print("API returned " + str(len(raw_payload)) + " rate records")
    print(raw_payload)

    task_instance = context["ti"]
    task_instance.xcom_push(key="raw_rates", value=raw_payload)

    return len(raw_payload)


# ----------------------------------------------------------------------
# Task 2: Transform
# ----------------------------------------------------------------------

def transform_exchange_rates(**context):
    """Reshape every raw record into the document layout we want to store."""

    task_instance = context["ti"]
    raw_payload = task_instance.xcom_pull(
        task_ids="extract_currency_rates",
        key="raw_rates",
    )

    if raw_payload is None:
        raise ValueError("No raw_rates value was found in XCom")

    # UTC timestamp, written without the offset so it matches the required format
    current_time = datetime.now(timezone.utc)
    retrieved_at = current_time.replace(microsecond=0, tzinfo=None).isoformat()

    transformed_records = []
    for record in raw_payload:
        base_currency = record.get("base")
        target_currency = record.get("quote")
        exchange_rate = record.get("rate")
        rate_date = record.get("date")

        # Skip anything that came back incomplete instead of storing a broken row
        if base_currency is None or target_currency is None:
            print("Skipping a record with a missing currency field")
            continue
        if exchange_rate is None or rate_date is None:
            print("Skipping " + str(base_currency) + " to " + str(target_currency) + " because of a missing rate or date")
            continue

        document = {
            "base_currency": base_currency,
            "target_currency": target_currency,
            "exchange_rate": float(exchange_rate),
            "rate_date": rate_date,
            "source": SOURCE_NAME,
            "retrieved_at": retrieved_at,
        }

        transformed_records.append(document)

    print("Transformed " + str(len(transformed_records)) + " records")
    for index in range(len(transformed_records)):
        print(transformed_records[index])

    task_instance.xcom_push(key="transformed_rates", value=transformed_records)

    return len(transformed_records)


# ----------------------------------------------------------------------
# Task 3: Load
# ----------------------------------------------------------------------

def load_rates_into_mongodb(**context):
    """Write the transformed documents into MongoDB."""

    task_instance = context["ti"]
    transformed_records = task_instance.xcom_pull(
        task_ids="transform_exchange_rates",
        key="transformed_rates",
    )

    if transformed_records is None or len(transformed_records) == 0:
        raise ValueError("Nothing to load, transformed_rates was empty")

    client = MongoClient(MONGO_URI)

    try:
        database = client[MONGO_DB_NAME]
        collection = database[MONGO_COLLECTION_NAME]

        # One rate per currency pair per day, so a rerun updates instead of duplicating
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
            name="unique_pair_per_day",
        )

        operations = []
        for index in range(len(transformed_records)):
            document = transformed_records[index]
            filter_query = {
                "base_currency": document["base_currency"],
                "target_currency": document["target_currency"],
                "rate_date": document["rate_date"],
            }
            operations.append(UpdateOne(filter_query, {"$set": document}, upsert=True))

        result = collection.bulk_write(operations, ordered=False)

        inserted_count = result.upserted_count
        updated_count = result.modified_count

        print("Inserted " + str(inserted_count) + " new documents")
        print("Updated " + str(updated_count) + " existing documents")
        print("Collection now holds " + str(collection.count_documents({})) + " documents")

        return inserted_count + updated_count

    finally:
        client.close()


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------

default_args = {
    "owner": "mehrmah",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_rates_etl",
    description="Daily EUR exchange rates from Frankfurter into MongoDB",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["etl", "currency", "mongodb"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates,
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
    )

    load_task = PythonOperator(
        task_id="load_rates_into_mongodb",
        python_callable=load_rates_into_mongodb,
    )

    extract_task >> transform_task >> load_task
