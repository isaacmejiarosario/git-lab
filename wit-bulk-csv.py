#!/usr/bin/env python3
#
#   bulk-csv.py  written by Steve Meuse (smeuse@kentik.com) 2022 Mar 1
#
#   This simple python script takes a .csv file with two columns (populator, cidr)
#   which allows you to use the Kentik bulk API to quickly load custom dimension
#   populator data. It then will aggregate all the IPs in the list of adresses for each
#   populator for efficiency.
#
#   This script uses the Kentik bulk API written by Blake Caldwell which can be located at:
#   https://github.com/kentik/kentikapi-py/tree/master/kentikapi/v5

from collections import defaultdict
import netaddr
from kentikapi.v5 import tagging
import csv
import sys
import json
import time
import os
import stat
from os.path import expanduser

################################
# This section are the user editable variables the script relies upon
#
# The file containing the populators
input_file = "clientes_b2b.csv"
#
# "pupulator_name" and "populator_data" should be the column headers for the CSV file.
#
populator_name = "customer"
populator_data = "ip_address"
#
# This script automatically creates both the source and destination populators for a given custom dimension.
# In the Kentik portal, you need to create two custom dimensions. The database fields must be in the format of
# "c_dimension_dst" and "c_dimension_src". The variable below labeled "dimension_name_prefix" will be the name of the
# dimension minus "_dst" and "_src". The script will add the populators and IP blocks to both the source and destination
# dimensions.
#
dimension_name_prefix = "c_clientes"
#
#  End of user variables
#################################


dimension_dst = "{}_dst".format(dimension_name_prefix)
dimension_src = "{}_src".format(dimension_name_prefix)
dimension_either = "{}_any".format(dimension_name_prefix)

customers = defaultdict(list)
agglist = []



# initialize a batch that will replace all populators
batch_dst = tagging.Batch(True)
batch_src = tagging.Batch(True)
batch_either = tagging.Batch(True)

# Use the CSV reader library to read in the CSV file and send the data to the batch API
# The column headings should be labeled cidr and market
# Rather than add populators for each cidr block, group them together into a single entry by unique populator name.

with open(input_file, mode='r', encoding='utf-8-sig') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        customer = row[populator_name]
        ip_address = row[populator_data]
        customers[customer].append(ip_address)

# Mow iterate through each list of prefixes, per-populator, and aggregate them using
# the netaddr.cidr_merge function. We're creating
# two batches (for src and dst) so we need two criteria, addresses and populators)

for item in customers.items():
    populator = item[0]
    crit_dst = tagging.Criteria("dst")
    crit_src = tagging.Criteria("src")
    crit_either = tagging.Criteria("either")
    agglist = netaddr.cidr_merge(item[1])
    for i in agglist:
        crit_dst.add_ip_address(str(i.cidr))
        crit_src.add_ip_address(str(i.cidr))
        crit_either.add_ip_address(str(i.cidr))
    batch_dst.add_upsert(populator, crit_dst)
    batch_src.add_upsert(populator, crit_src)
    batch_either.add_upsert(populator, crit_either)

#
# Prep our credentials that we read in via the get_creds() function
#
#api = get_creds()['api']
#email = get_creds()['email']
api= "3be2c2049219b7fcd592bb9e67712dc4"
email= "imejia-wit@witintl.com"
client = tagging.Client(email, api)   # (set your credentials)

#  Submit both batches
guid_dst = client.submit_populator_batch(dimension_dst, batch_dst)
guid_src = client.submit_populator_batch(dimension_src, batch_src)
guid_either = client.submit_populator_batch(dimension_either, batch_either)

#  Wait and display the results
for x in range(1, 12):
    time.sleep(5)
    status = client.fetch_batch_status(guid_dst)
    if status.is_finished():
        print("is_finished: %s" % str(status.is_finished()))
        print("upsert_error_count: %s" % str(status.invalid_upsert_count()))
        print("delete_error_count: %s" % str(status.invalid_delete_count()))
        print()
        print(status.pretty_response())
        break

#  Wait and display the results
for x in range(1, 12):
    time.sleep(5)
    status = client.fetch_batch_status(guid_src)
    if status.is_finished():
        print("is_finished: %s" % str(status.is_finished()))
        print("upsert_error_count: %s" % str(status.invalid_upsert_count()))
        print("delete_error_count: %s" % str(status.invalid_delete_count()))
        print()
        print(status.pretty_response())
        break

#  Wait and display the results
for x in range(1, 12):
    time.sleep(5)
    status = client.fetch_batch_status(guid_either)
    if status.is_finished():
        print("is_finished: %s" % str(status.is_finished()))
        print("upsert_error_count: %s" % str(status.invalid_upsert_count()))
        print("delete_error_count: %s" % str(status.invalid_delete_count()))
        print()
        print(status.pretty_response())
        break

