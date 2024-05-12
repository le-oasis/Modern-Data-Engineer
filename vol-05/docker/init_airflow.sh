#!/bin/bash

airflow connections add 'oasis_con_postgres' \
    --conn-type 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-host 'oasispostgresdb' \
    --conn-port '5432' \
    --conn-schema 'airflow'

airflow connections add 'spark_connect' \
    --conn-type 'spark' \
    --conn-host 'spark://oasissparkm' \
    --conn-port '7077' \
    --conn-extra '{"queue": "root.default"}'