#!/usr/bin/env python

from twitterneo4j.tweets import tweets
import twitterneo4j.variables as variables

import time
import threading


def start(cpu_number):

    try:
        threads = []

        # Create the requested number of threads
        for i in range(variables.threads_per_cpu):
            thread_num = (cpu_number * variables.threads_per_cpu) + i
            t = threading.Thread(target=read_tweets_worker, args=(thread_num,))
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
        print "Exception: %s (%s)" % (str(e), cpu_number)


def read_tweets_worker(thread_num):
    tweets.read_tweets(thread_num)
