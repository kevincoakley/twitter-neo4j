#!/usr/bin/env python

import yaml
import multiprocessing
import twitterneo4j.variables as variables


def configure(config_yaml_file):

    #
    # Read the config
    #
    config_yaml = open(config_yaml_file, "r")
    config = yaml.load(config_yaml)
    config_yaml.close()

    # Required Configuration
    variables.neo4j_host = config["neo4j_host"]
    variables.neo4j_username = config["neo4j_username"]
    variables.neo4j_password = config["neo4j_password"]
    variables.graph_url = config["graph_url"]
    variables.cypher_query = config["cypher_query"]

    if config["cpu_count"] == "all":
        variables.cpu_count = multiprocessing.cpu_count()
    else:
        variables.cpu_count = int(config["cpu_count"])

    variables.threads_per_cpu = config["threads_per_cpu"]
    variables.tweet_path = config["tweet_path"]

    # Optional Configuration
    if "uniqueness_constraints" in config:
        variables.uniqueness_constraints = config["uniqueness_constraints"]

    if "indexes" in config:
        variables.indexes = config["indexes"]
