#!/usr/bin/env python

from twitterneo4j.neo4j import neo4j
import twitterneo4j.variables as variables

import os
import gzip
import json
import time
import yaml
from datetime import datetime


def get_tweet_file_list():

    #
    # Get the list of tweet files to import
    #
    for f in os.listdir(variables.tweet_path):
        if os.path.isfile(variables.tweet_path + f) and f != ".file_position":
            variables.tweet_files.append(os.path.join(variables.tweet_path, f))

    # Sort the filenames alphabetically
    variables.tweet_files = sorted(variables.tweet_files)


def read_tweets(thread_num):

    # If the thread number is greater than the number of tweet files, then do nothing
    if len(variables.tweet_files) > thread_num:
        tweet_file = variables.tweet_files[thread_num]

        if tweet_file.endswith(".gz"):
            read_tweets_gzip(tweet_file, thread_num)
        else:
            read_tweets_plain_text(tweet_file, thread_num)


def read_tweets_plain_text(tweet_file, thread_num):

    count = 0
    query_time_total = 0

    with open(tweet_file) as tweet_source:

        position, line_num = get_tweet_file_position(tweet_file)

        if position is not None and line_num is not None:
            tweet_source.seek(position)
            count = line_num

        for line in tweet_source:

            # Check if the line is valid JSON. Since the file is opened as an iterator that uses
            # a read-ahead buffer to increase efficiency, the file pointer advances in large steps
            # across the file as the script loops over the lines. Because the file pointer
            # advances in large stops, the file.tell() method may report the position in the middle
            # of a line, instead of at the start of a line, which could cause the JSON to be invalid
            # on the first tweet when resuming. Since the script only saves the file.tell()
            # information every 1000 lines, this first error can be ignored since the JSON should
            # have already been read and imported.
            try:
                json.loads(line)
            except ValueError:
                print "Invalid JSON:\n%s" % line
                continue

            query_time_total = process_tweet(tweet_file, line, count, query_time_total, thread_num)

            if count % 1000 == 0 and count is not 0:
                save_tweet_file_position(tweet_file, tweet_source.tell(), count)

            count += 1

    tweet_source.close()


def read_tweets_gzip(tweet_file, thread_num):

    count = 0
    query_time_total = 0

    with gzip.open(tweet_file) as tweet_source:
        for line in tweet_source:

            query_time_total = process_tweet(tweet_file, line, count, query_time_total, thread_num)

            if count % 1000 == 0 and count is not 0:
                save_tweet_file_position(tweet_file, tweet_source.tell(), count)

            count += 1

    tweet_source.close()


def process_tweet(tweet_file, line, count, query_time_total, thread_num):

    t0 = time.time()
    neo4j.json_cypher_query(line, thread_num)
    t1 = time.time()
    query_time_total += (t1 - t0)

    if count % 1000 == 0 and count is not 0:
        average_query_time = (query_time_total / 1000)

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

        query_time_total = 0

    return query_time_total


def get_tweet_file_position(tweet_file):

    tweet_position_file = variables.tweet_path + ".file_position"

    if os.path.isfile(tweet_position_file):
        tweet_position_yaml = open(tweet_position_file, "r")
        tweet_position = yaml.load(tweet_position_yaml)
        tweet_position_yaml.close()

        if tweet_file in tweet_position:
            return tweet_position[tweet_file]["position"], tweet_position[tweet_file]["line_num"]

    return None, None


def save_tweet_file_position(tweet_file, position, line_num):

    tweet_position_file = variables.tweet_path + ".file_position"

    tweet_position = {}

    if os.path.isfile(tweet_position_file):
        tweet_position_yaml = open(tweet_position_file, "r")
        tweet_position = yaml.load(tweet_position_yaml)
        tweet_position_yaml.close()

    tweet_position[tweet_file] = {"position": position,
                                  "line_num": line_num,
                                  "datetime": datetime.now()}

    with open(tweet_position_file, 'w') as tweet_position_yaml:
        tweet_position_yaml.write(yaml.dump(tweet_position, default_flow_style=False))
    tweet_position_yaml.close()
