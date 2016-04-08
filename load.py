#!/usr/bin/env python

import os
from py2neo import Graph, authenticate
import argparse
import threading
import multiprocessing
import yaml
import json
import gzip
import time
from datetime import datetime


def load_tweets_worker(thread_num):

    # If the thread number is greater than the number of tweet files, then do nothing
    if len(tweet_files) > thread_num:

        count = 0
        time_total = 0

        with gzip.open(tweet_files[thread_num]) as f:
            for line in f:
                t0 = time.time()
                submit_json_to_neo4j(line, thread_num)
                t1 = time.time()
                time_total += (t1-t0)

                if count % 1000 == 0:
                    print "\n%s" % str(datetime.now())
                    print "Filename: %s" % tweet_files[thread_num]
                    print "Thread %s Count: %s" % (thread_num, count)
                    print "Average Query Time: %s" % (time_total / 1000)
                    time_total = 0

                count += 1

        f.close()


def submit_json_to_neo4j(tweet, thread_num):

    tweet_json = json.loads(tweet)

    # Ignore invalid tweet data
    if "limit" in tweet_json:
        return

    try:
        graph.cypher.run(query, json=tweet_json)
    except Exception as e:
        print "Exception: %s (%s)" % (str(e), thread_num)
        # Slow down if too many files are open
        if str(e) == "Too many open files":
            print "Pausing thread %s" % thread_num
            time.sleep(5)
        if str(e) == "Cannot merge node using null property value for id":
            print "Thread: %s \n %s" % (thread_num, tweet_json)


def start_worker(processor_num):

    try:
        threads = []

        # Create the requested number of threads
        for j in range(threads_per_cpu):
            thread_num = (processor_num * threads_per_cpu) + j
            t = threading.Thread(target=load_tweets_worker, args=(thread_num,))
            t.daemon = True
            threads.append(t)
            t.start()
            time.sleep(1)

        # Keep looping until all of the threads have died
        while True:
            need_return = True

            for thread in threads:
                if thread.isAlive() is True:
                    need_return = False

            if need_return is True:
                break
    except Exception as e:
        print "Exception: %s (%s)" % (str(e), processor_num)


def pool_function(processor_num):

    try:
        return start_worker(processor_num)
    except KeyboardInterrupt:
        return "KeyboardException"


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-c",
                        metavar="config_yaml",
                        dest="config_yaml",
                        help="Configuration YAML File.",
                        required=True)

    args = vars(parser.parse_args())

    #
    # Read the config
    #
    config_yaml = open(args["config_yaml"], "r")
    config = yaml.load(config_yaml)

    authenticate(config["neo4j_host"], config["neo4j_username"], config["neo4j_password"])
    graph = Graph(config["graph_url"])
    query = config["cypher_query"]

    if config["cpu_count"] == "all":
        cpu_count = multiprocessing.cpu_count()
    else:
        cpu_count = int(config["cpu_count"])

    threads_per_cpu = config["threads_per_cpu"]
    tweet_path = config["tweet_path"]

    #
    # Get the list of tweet files to import
    #
    tweet_files = []

    for f in os.listdir(tweet_path):
        tweet_files.append(os.path.join(tweet_path, f))

    # Sort the filenames alphabetically
    tweet_files = sorted(tweet_files)

    #
    # Create Uniqueness Constraint
    #
    if "uniqueness_constraints" in config:
        for uniqueness_constraint in config["uniqueness_constraints"]:
            if uniqueness_constraint["property"] not in graph.schema.get_uniqueness_constraints(uniqueness_constraint["label"]):
                print "Creating Uniqueness Constraint: :%s(%s)" % (uniqueness_constraint["label"], uniqueness_constraint["property"])
                graph.schema.create_uniqueness_constraint(uniqueness_constraint["label"], uniqueness_constraint["property"])

    #
    # Create Indexes
    #
    if "indexes" in config:
        for index in config["indexes"]:
            if index["property"] not in graph.schema.get_indexes(index["label"]):
                print "Creating Index: :%s(%s)" % (index["label"], index["property"])
                graph.schema.create_index(index["label"], index["property"])

    # HERE
    pool = multiprocessing.Pool(cpu_count)

    jobs = []

    try:
        for i in range(cpu_count):
            jobs.append(pool.apply_async(pool_function, args=(i,)))
        pool.close()
        pool.join()
    except KeyboardInterrupt:
        pool.terminate()

    for i in jobs:
        if i.successful():
            print i.get()
        else:
            print "Job failed: %s" % str(i)
