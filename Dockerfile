FROM quay.io/astronomer/astro-runtime:12.7.1

# Meta details for the image
LABEL project="currency-exchange-etl"
LABEL role="airflow-runtime"
LABEL maintainer="Mehrmah Khan"
LABEL description="Airflow ETL that loads Frankfurter exchange rates into MongoDB"
