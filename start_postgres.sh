#!/bin/bash
set -e
source .env

tool="dlt-postgres"
vol="dlt-postgres_data"

id="$(docker ps -a -q -f name=${tool})"
vol_id="$(docker volume ls -q -f name=${vol})"

if [ ! -z "${id}" ]; then
    echo "Docker ${id} already exists for ${tool}"
    docker start $id > /dev/null
else
    if [ -z "${vol_id}" ]; then
        echo "Creating new volume ${vol} for ${tool}"
        docker volume create $vol  > /dev/null
    fi

    echo "Starting new docker for ${tool}"
    docker run \
        --name $tool \
        -p ${POSTGRES_PORT:-15432}:5432 \
        -e POSTGRES_USER=${POSTGRES_USER} \
        -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
        -v $vol:/var/lib/postgresql/data \
        -v ./setup_db.sql:/docker-entrypoint-initdb.d/10_setup_db.sql \
        -d \
        postgres:16.9 > /dev/null

    id="$(docker ps -a -q -f name=${tool})"
fi

echo "Docker ${id} (re-)started, to stop it run:"
echo -e "\033[1;33mdocker stop ${tool}\033[0m"
