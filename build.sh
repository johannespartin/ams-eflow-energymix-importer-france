#!/bin/sh
docker build -t ams-eflow-energymix-importer-france .
docker tag ams-eflow-energymix-importer-france:latest 768867912825.dkr.ecr.eu-central-1.amazonaws.com/ams-eflow-energymix-importer-france:latest
docker push 768867912825.dkr.ecr.eu-central-1.amazonaws.com/ams-eflow-energymix-importer-france:latest