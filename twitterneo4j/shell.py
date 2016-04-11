#!/usr/bin/env python

import twitterneo4j.arguments as arguments
import twitterneo4j.configure as configure
import twitterneo4j.multi.pool as multi_pool
import twitterneo4j.neo4j.neo4j as neo4j
import twitterneo4j.tweets.tweets as tweets
import twitterneo4j.variables as variables

from py2neo import Graph, authenticate
import multiprocessing


def main():

    # Read command line arguments
    args = arguments.parse_arguments()

    variables.stats_log_filename = args["stats_log_filename"]

    # Read config.yaml
    configure.configure(args["config_yaml"])

    # Check the supplied credentials with the neo4j server, replace the default username and
    # password if necessary
    neo4j.check_login()

    # Authenticate to neo4j and create a neo4j graph object
    authenticate(variables.neo4j_host, variables.neo4j_username, variables.neo4j_password)
    variables.graph_object = Graph(variables.graph_url)

    # Get the list of tweet JSON file to load into Neo4j
    tweets.get_tweet_file_list()

    # Create the neo4j uniqueness constraints and indexes before importing the tweets
    neo4j.create_uniqueness_constraints()
    neo4j.create_indexes()

    # Create a multiprocessing pool using the number of cpus specified in the config
    pool = multiprocessing.Pool(variables.cpu_count)

    jobs = []

    try:
        # Create a pool job for the number of cpus specified in the config
        for i in range(variables.cpu_count):
            jobs.append(pool.apply_async(multi_pool.start_workers, args=(i,)))
        pool.close()
        pool.join()
    except KeyboardInterrupt:
        pool.terminate()

    # Keep looping through the jobs to check if they have completed successfully
    for i in jobs:
        if i.successful():
            print i.get()
        else:
            print "Job failed: %s" % str(i)
