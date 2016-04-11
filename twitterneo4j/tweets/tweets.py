#!/usr/bin/env python

from twitterneo4j.neo4j import neo4j
import twitterneo4j.variables as variables

import os
import gzip
import time
from datetime import datetime


def get_tweet_file_list():

    #
    # Get the list of tweet files to import
    #
    for f in os.listdir(variables.tweet_path):
        variables.tweet_files.append(os.path.join(variables.tweet_path, f))

    # Sort the filenames alphabetically
    variables.tweet_files = sorted(variables.tweet_files)


def read_tweets(thread_num):

    # If the thread number is greater than the number of tweet files, then do nothing
    if len(variables.tweet_files) > thread_num:

        count = 0
        time_total = 0

        tweet_file = variables.tweet_files[thread_num]

        with gzip.open(tweet_file) as tweet_source:
            for line in tweet_source:
                t0 = time.time()
                neo4j.json_cypher_query(line, thread_num)
                t1 = time.time()
                time_total += (t1 - t0)

                if count % 1000 == 0 and count is not 0:
                    average_query_time = (time_total / 1000)

                    print "\n%s" % str(datetime.now())
                    print "Filename: %s" % tweet_file
                    print "Thread %s Count: %s" % (thread_num, count)
                    print "Average Query Time: %s" % average_query_time

                    # If the stats_log_filename has been defined, then write the average query
                    # time to a csv file
                    if variables.stats_log_filename is not None:
                        stats_log = open(variables.stats_log_filename, "a")
                        stats_log.write("%s,%s,%s,%s\n" % (datetime.now(), tweet_file, count,
                                                           average_query_time))
                        stats_log.close()

                    time_total = 0

                count += 1

        tweet_source.close()
