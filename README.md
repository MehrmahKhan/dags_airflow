# Currency Exchange Rate ETL with Airflow and MongoDB

An Airflow pipeline, run locally through the Astro CLI, that pulls daily
currency exchange rates from the Frankfurter public API, reshapes them, and
stores them in MongoDB. Mongo Express runs as a second Docker service so the
stored documents can be inspected from a browser.

## What the pipeline does

The DAG `currency_rates_etl` runs three tasks in order:

1. `extract_currency_rates` calls the Frankfurter API for EUR against USD, GBP
   and JPY, and pushes the untouched response into XCom under the key
   `raw_rates`.
2. `transform_exchange_rates` pulls `raw_rates`, turns every record into one
   flat document, and pushes the list under the key `transformed_rates`.
3. `load_rates_into_mongodb` pulls `transformed_rates` and upserts every
   document into the `exchange_rates` collection of the `currency_db` database.

All three tasks use named XComs, so each push and pull names the key it is
using rather than relying on the default return value.

### Document stored in MongoDB

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1196,
  "rate_date": "2026-11-09",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-11T15:42:24"
}
```

A unique index sits on `base_currency`, `target_currency` and `rate_date`, so
running the DAG twice on the same day updates the existing documents instead of
creating duplicates.

## Project layout

```
.
├── dags/
│   └── currency_rates_etl.py      the three task pipeline
├── tests/
│   └── test_dag_integrity.py      checks the DAG loads and has three tasks
├── Dockerfile                     Astro runtime image plus meta labels
├── docker-compose.override.yml    MongoDB and Mongo Express services
├── requirements.txt               pymongo and requests
└── README.md
```

## Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| Airflow webserver | astro-runtime 12.7.1 | 8080 | DAG UI |
| mongodb | mongo:7.0 | 27017 | primary database |
| mongo-express | mongo-express:1.0.2 | 8081 | database admin UI |

### Docker networking

Both extra services join the `airflow` bridge network that Astro creates for
the project. Because they share that network, the DAG reaches the database by
the service name `mongodb` rather than by an IP address, and Mongo Express
reaches it the same way.

### Persistent storage

MongoDB mounts two named volumes:

* `mongo_data` on `/data/db` holds the actual collections
* `mongo_config` on `/data/configdb` holds server configuration

Data written by the pipeline stays in place across `astro dev restart` and
across a container rebuild. It is only removed if the volumes are deleted on
purpose with `docker volume rm`.

## Running it

You need Docker Desktop running and the Astro CLI installed.

```bash
astro dev start
```

The first start takes a few minutes while images are pulled. When it finishes:

* Airflow UI: http://localhost:8080 with username `admin` and password `admin`
* Mongo Express: http://localhost:8081, which opens straight to the database list

In the Airflow UI, unpause `currency_rates_etl` and press the play button to
trigger a run. Once the three tasks go green, open Mongo Express, click into
`currency_db`, then `exchange_rates`, and the documents will be listed there.

Useful commands while working:

```bash
astro dev restart          # rebuild after changing requirements or the Dockerfile
astro dev logs --scheduler # scheduler logs
astro dev stop             # stop everything, volumes are kept
docker ps                  # confirm all containers including mongodb are up
```

## Checking the data from the shell

```bash
docker exec -it currency_mongodb mongosh -u mongo_user -p mongo_pass --authenticationDatabase admin
```

Then inside the mongosh prompt:

```javascript
use currency_db
db.exchange_rates.find().pretty()
db.exchange_rates.countDocuments()
```

## Credentials

These are local development credentials and they are written directly into the
compose file and the DAG so the project runs with no extra setup.

* MongoDB user `mongo_user`, password `mongo_pass`
* Mongo Express runs with its basic auth turned off, since it is only bound to this machine

For anything beyond a local assignment these would move into environment
variables or an Airflow connection instead of sitting in the source.
