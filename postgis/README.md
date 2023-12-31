# Culvert Vision MVP - PostGIS

Containerized PostGIS database that serves as the central repository for vector data used or generated by the Culvert Vision MVP project.

## Setup

A Makefile is provided to expose convenience commands for starting and stopping the database services.

The `start_postgis_prod` command requires an environment file defined in this directory named `prod.env`. The file must define the variables `POSTGIS_USER` and `POSTGIS_PASS`.

```bash
$ make
Available rules:

start_postgis_dev   Launch development PostGIS database
start_postgis_prod  Launch production PostGIS database
stop_postgis_dev    Shutdown development PostGIS database
stop_postgis_prod   Shutdown production PostGIS databaseAvailable rules:
```
