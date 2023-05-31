#!/bin/bash

INFRA_DIR="../infrastructure"

docker compose --project-name pulsar-nifi --file $INFRA_DIR/pulsar-and-nifi.yml down