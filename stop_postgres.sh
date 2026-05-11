#!/bin/bash
set -e

tool="dlt-postgres"
vol="dlt-postgres_data"
clear=$1

id="$(docker ps -q -f name=${tool})"
vol_id="$(docker volume ls -q -f name=${vol})"

if [ ! -z "${id}" ]; then
    echo "Stopping docker ${id} for ${tool}"
    docker stop $id > /dev/null
else
    echo "No running dockers for ${tool} were found"
fi

if [ "${clear}" == "-v" ]; then
    echo "Removing docker ${tool}"
    docker rm $tool > /dev/null

    echo "Removing volume ${vol}"
    docker volume rm $vol > /dev/null
fi