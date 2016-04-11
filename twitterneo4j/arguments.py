#!/usr/bin/env python

import argparse


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c",
                        metavar="config_yaml",
                        dest="config_yaml",
                        help="Configuration YAML File.",
                        required=True)

    parser.add_argument("-l",
                        metavar="stats_log_filename",
                        dest="stats_log_filename",
                        default=None,
                        help="Filename for the stats log file.",
                        required=False)

    return vars(parser.parse_args())
