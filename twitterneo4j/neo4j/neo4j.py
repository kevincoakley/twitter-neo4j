#!/usr/bin/env python

import twitterneo4j.variables as variables

import sys
import json
import time
from py2neo import ServiceRoot
from py2neo.password import UserManager


def check_login():

    try_default_password = False

    user_manager = UserManager.for_user(ServiceRoot("http://" + variables.neo4j_host),
                                        variables.neo4j_username, variables.neo4j_password)

    try:
        user_manager.password_manager
    except Exception as e:
        if str(e.__class__.__name__) is "Unauthorized":
            print "Unauthorized, trying default password"
            try_default_password = True

    if try_default_password:
        user_manager = UserManager.for_user(ServiceRoot("http://" + variables.neo4j_host),
                                            variables.neo4j_username, "neo4j")

        try:
            password_manager = user_manager.password_manager

            if password_manager.change(variables.neo4j_password):
                print("Password change succeeded")
            else:
                print("Password change failed")
                sys.exit(2)
        except Exception as e:
            print str(e.__class__.__name__)
            sys.exit(2)


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
        variables.graph_object.cypher.run(variables.cypher_query, json=tweet_json)
    except Exception as e:
        print "Exception: %s (%s)" % (str(e), thread_num)
        # Slow down if too many files are open
        if str(e) == "Too many open files":
            print "Pausing thread %s" % thread_num
            time.sleep(5)
        if str(e) == "Cannot merge node using null property value for id":
            print "Thread: %s \n %s" % (thread_num, tweet_json)
