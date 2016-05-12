#!/usr/bin/env python

import twitterneo4j.variables as variables

import sys
import json
import time
import requests


def check_login():

    http = "https"

    # Check if the server supports https, fall back to http if it doesn't
    try:
        requests.get("https://%s/user/%s" % (variables.neo4j_host, variables.neo4j_username))
    except requests.exceptions.SSLError:
        http = "http"

    r = requests.get("%s://%s/user/%s" % (http, variables.neo4j_host, variables.neo4j_username),
                     auth=(variables.neo4j_username, variables.neo4j_password))
    # If the password is incorrect, try the default password before exiting
    if r.status_code != requests.codes.ok:
        # Try default password
        r = requests.get("%s://%s/user/%s" % (http, variables.neo4j_host, variables.neo4j_username),
                         auth=(variables.neo4j_username, "neo4j"))
        # If the default password is correct, then change the password to what is in the config
        if r.status_code == requests.codes.ok:
            payload = {'password': variables.neo4j_password}

            r = requests.post("%s://%s/user/%s/password" % (http, variables.neo4j_host,
                                                              variables.neo4j_username),
                              auth=(variables.neo4j_username, "neo4j"), data=payload)
            if r.status_code == requests.codes.ok:
                print "Password Changed"
            else:
                print "Password Change Failed: %s" % r.text
                sys.exit(2)
        else:
            print "Invalid Username and/or Password"
            sys.exit(2)
    else:
        return


def create_uniqueness_constraints():

    if len(variables.uniqueness_constraints) > 0:
        for uniqueness_constraint in variables.uniqueness_constraints:
            if uniqueness_constraint["property"] not in variables.graph_object.schema.\
                    get_uniqueness_constraints(uniqueness_constraint["label"]):
                print "Creating Uniqueness Constraint: :%s(%s)" % \
                      (uniqueness_constraint["label"], uniqueness_constraint["property"])
                variables.graph_object.schema.\
                    create_uniqueness_constraint(uniqueness_constraint["label"],
                                                 uniqueness_constraint["property"])


def create_indexes():

    if len(variables.indexes) > 0:
        for index in variables.indexes:
            if index["property"] not in variables.graph_object.schema.get_indexes(index["label"]):
                print "Creating Index: :%s(%s)" % (index["label"], index["property"])
                variables.graph_object.schema.create_index(index["label"], index["property"])


def json_cypher_query(tweet, thread_num):

    tweet_json = json.loads(tweet)

    # Ignore invalid tweet data
    if "limit" in tweet_json:
        return

    try:
        variables.graph_object.run(variables.cypher_query, json=tweet_json)
    except Exception as e:
        print "Exception: %s (%s)" % (str(e), thread_num)
        # Slow down if too many files are open
        if str(e) == "Too many open files":
            print "Pausing thread %s" % thread_num
            time.sleep(5)
        if str(e) == "Cannot merge node using null property value for id":
            print "Thread: %s \n %s" % (thread_num, tweet_json)
