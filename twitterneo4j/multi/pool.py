#!/usr/bin/env python

import twitterneo4j.multi.worker as worker


def start_workers(cpu_number):

    try:
        return worker.start(cpu_number)
    except KeyboardInterrupt:
        return "KeyboardException"
