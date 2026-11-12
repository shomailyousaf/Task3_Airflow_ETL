
import logging
from datetime import datetime, timezone

import pendulum
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator



API_URL = "https://api.frankfurter.dev/v2/rates"

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = "USD,GBP,JPY"

MONGO_DATABASE = "currency_db"
MONGO_COLLECTION = "exchange_rates"

default_args = {
    "owner": "airflow",
    "retries": 2,
}



def extract_exchange_rates(**context):

    logging.info("Starting currency exchange rate extraction")

    params = {
        "base": BASE_CURRENCY,
        "quotes": TARGET_CURRENCIES
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    api_data = response.json()

    if not isinstance(api_data, list) or not api_data:
        raise ValueError("No exchange rate data received from API")

    raw_rates = []

    for item in api_data:
        raw_rates.append({
            "base": item["base"],
            "quote": item["quote"],
            "rate": item["rate"],
            "date": item["date"]
        })

    
    context["ti"].xcom_push(
        key="raw_exchange_rates",
        value=raw_rates
    )

    logging.info("Extracted %s exchange rates", len(raw_rates))




def transform_exchange_rates(**context):

    logging.info("Starting exchange rate transformation")

    
    raw_rates = context["ti"].xcom_pull(
        task_ids="extract_rates",
        key="raw_exchange_rates"
    )

    if not raw_rates:
        raise ValueError("No raw exchange rate data found in XCom")

    retrieved_at = datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )

    transformed_rates = []

    for item in raw_rates:

        transformed_record = {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": float(item["rate"]),
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at
        }

        transformed_rates.append(transformed_record)

   
    context["ti"].xcom_push(
        key="transformed_exchange_rates",
        value=transformed_rates
    )

    logging.info(
        "Successfully transformed %s exchange rates",
        len(transformed_rates)
    )




def load_exchange_rates(**context):

    logging.info("Starting MongoDB data loading")

   
    transformed_rates = context["ti"].xcom_pull(
        task_ids="transform_rates",
        key="transformed_exchange_rates"
    )

    if not transformed_rates:
        raise ValueError("No transformed data found in XCom")

   
    mongo_uri = Variable.get("MONGO_URI")

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=10000
    )

    try:
       
        client.admin.command("ping")

        database = client[MONGO_DATABASE]
        collection = database[MONGO_COLLECTION]

       
        collection.create_index(
            [
                ("base_currency", 1),
                ("target_currency", 1),
                ("rate_date", 1)
            ],
            unique=True
        )

        inserted_or_updated = 0

        for record in transformed_rates:

            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"]
                },
                {"$set": record},
                upsert=True
            )

            inserted_or_updated += 1

        logging.info(
            "Successfully loaded %s records into MongoDB",
            inserted_or_updated
        )

    finally:
        client.close()



with DAG(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rate ETL pipeline using Frankfurter API and MongoDB",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 11, 11, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "Currency", "MongoDB"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_rates",
        python_callable=extract_exchange_rates
    )

    transform_task = PythonOperator(
        task_id="transform_rates",
        python_callable=transform_exchange_rates
    )

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_exchange_rates
    )

    
    extract_task >> transform_task >> load_task
