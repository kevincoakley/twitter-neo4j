#!/usr/bin/env bash

docker run --detach --publish=7474:7474 --publish=7687:7687 --ulimit=nofile=40000:40000 --name neo4j neo4j:3.0
