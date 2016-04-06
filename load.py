#!/usr/bin/env python

from py2neo import Graph, authenticate
import argparse
import threading
import multiprocessing
import yaml
import json
import gzip
import time
from datetime import datetime


def worker(thread_num):

    thread_num += 1
    count = 0

    with gzip.open("/Users/kcoakley/Downloads/election%03d.gz" % thread_num) as f:
        for line in f:
            callback(line, thread_num)

            if count % 1000 == 0:
                print "%s Thread %s Count: %s" % (str(datetime.now()), thread_num, count)

            count += 1

    f.close()


def callback(tweet, thread_num):
    tweet_json = json.loads(tweet)

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

    threads_per_cpu = 1

    try:
        threads = []

        # Create the requested number of threads
        for j in range(threads_per_cpu):
            thread_num = (processor_num * threads_per_cpu) + j
            t = threading.Thread(target=worker, args=(thread_num,))
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
