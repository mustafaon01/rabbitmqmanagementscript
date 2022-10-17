#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Developed by Mustafa ONCU

import requests
import argparse
import os
from requests.auth import HTTPBasicAuth

h = '''
*****first of all you need to set environment variable*****
use below commands to run rabbitmq-management.py script\n

-s {name_of_source_vhost} choosing vhost which exists in prod rabbitmq(it is not required, default="guest")\n
-t {name_of_target_vhost} if exists in local it chooses, otherwise it create on local rabbitmq(it is not required, 
default="guest")\n
\n-t {name_of_vhost} create all (create and duplicate all queues, exchanges and bindings from prod to local)\n
-
'''
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--svhost", default="guest", required=False, help="determines the source vhost")
parser.add_argument("-t", "--tvhost", default="", required=False, help="determines the target vhost")
parser.add_argument("-k", "--keyword", nargs="?", default="", required=False,
                    help="determines the variable we want the queue name to be in")
subparser = parser.add_subparsers(dest="option", help=h)
parser_list = subparser.add_parser('list')
parser_list.add_argument("list_type", nargs="?", default="all", choices=['all', 'queues', 'vhosts', 'exchanges'],
                         help="give the wanted type of list ")
parser_create = subparser.add_parser('create')
parser_create.add_argument("create_type", nargs="?", default="all", choices=['all', 'queues', 'vhosts', 'exchanges'],
                           help="create the wanted types ")
parser_delete = subparser.add_parser('delete')
parser_delete.add_argument("delete_type", nargs="?", default="all", choices=['all', 'queues', 'vhosts', 'exchanges'],
                           help="remove the wanted types ")
args = parser.parse_args()

RABBIT_API_URL_LOCAL = "http://localhost:15672/api"
RABBIT_API_URL_PROD = "http://localhost:15672/api"
SOURCE_VHOST = args.svhost
TARGET_VHOST = args.tvhost
KEY_WORD = args.keyword
RABBIT_USERNAME = os.getenv("RABBIT_USER")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD")
RABBIT_PROD_USERNAME = os.getenv("RABBIT_USER_PROD")
RABBIT_PROD_PASSWORD = os.getenv("RABBIT_PASSWORD_PROD")
vhosts = []
queues = []


def start():
    print("## key word:", KEY_WORD)
    if args.option == 'create':
        if TARGET_VHOST not in vhosts:
            create_vhost(RABBIT_API_URL_LOCAL, TARGET_VHOST)
        if args.create_type == "all":
            create_all(RABBIT_API_URL_PROD, RABBIT_API_URL_LOCAL, SOURCE_VHOST, TARGET_VHOST, KEY_WORD)
        elif args.create_type == "queues":
            existing_queue_list = get_queue_list(RABBIT_API_URL_PROD, SOURCE_VHOST, KEY_WORD)
            create_queues(existing_queue_list, RABBIT_API_URL_LOCAL, TARGET_VHOST)
        elif args.create_type == "vhosts":
            create_vhost(RABBIT_API_URL_LOCAL, TARGET_VHOST)
        elif args.create_type == "exchanges":
            existing_exchange_list = get_exchanges_list(RABBIT_API_URL_PROD, SOURCE_VHOST, KEY_WORD)
            create_exchanges(existing_exchange_list, RABBIT_API_URL_LOCAL, RABBIT_API_URL_PROD, TARGET_VHOST,
                             SOURCE_VHOST, KEY_WORD)
    elif args.option == "delete":
        if args.delete_type == "all":
            delete_all(RABBIT_API_URL_PROD, RABBIT_API_URL_LOCAL, SOURCE_VHOST, TARGET_VHOST, KEY_WORD)
        elif args.delete_type == "vhosts":
            delete_vhost(RABBIT_API_URL_LOCAL, TARGET_VHOST)
        elif args.delete_type == "queues":
            existing_queue_list = get_queue_list(RABBIT_API_URL_PROD, SOURCE_VHOST, KEY_WORD)
            delete_all_queues(RABBIT_API_URL_LOCAL, existing_queue_list, TARGET_VHOST)
        elif args.delete_type == "exchanges":
            existing_exchange_list = get_exchanges_list(RABBIT_API_URL_LOCAL, TARGET_VHOST, KEY_WORD)
            delete_all_exchanges(RABBIT_API_URL_LOCAL, existing_exchange_list, TARGET_VHOST, KEY_WORD)
    elif args.option == "list":
        if args.list_type == "all":
            list_all(RABBIT_API_URL_LOCAL, TARGET_VHOST, KEY_WORD)
        elif args.list_type == "queues":
            get_queue_list(RABBIT_API_URL_LOCAL, TARGET_VHOST, KEY_WORD)
        elif args.list_type == "vhosts":
            get_vhosts_list(RABBIT_API_URL_LOCAL)
        elif args.list_type == "exchanges":
            get_exchanges_list(RABBIT_API_URL_LOCAL, TARGET_VHOST, KEY_WORD)


def create_all(prod_url, local_url, source_vhost, target_vhost, key_word):
    existing_queue_list = get_queue_list(prod_url, source_vhost, key_word)
    existing_exchange_list = get_exchanges_list(prod_url, source_vhost, key_word)
    create_queues(existing_queue_list, local_url, target_vhost)
    create_exchanges(existing_exchange_list, local_url, prod_url, target_vhost, source_vhost, key_word)


def delete_all(prod_url, local_url,  source_vhost, target_vhost, key_word):
    existing_queue_list = get_queue_list(prod_url, source_vhost, key_word)
    existing_exchange_list = get_exchanges_list(prod_url, source_vhost, key_word)
    delete_all_queues(local_url, existing_queue_list, target_vhost)
    delete_all_exchanges(local_url, existing_exchange_list, target_vhost, key_word)


def list_all(local_url, target_vhost, key_word):
    get_vhosts_list(local_url)
    get_queue_list(local_url, target_vhost, key_word)
    get_exchanges_list(local_url, target_vhost, key_word)


def build_binding_payload(binding):
    payload = {
        "vhost": binding["vhost"],
        "destination_type": binding["destination_type"],
        "routing_key": binding["routing_key"],
        "arguments": binding["arguments"],
        "properties_key": binding["properties_key"]
    }
    return payload


def build_exchange_payload(exchange):
    payload = {
        "arguments": exchange["arguments"],
        "auto_delete": exchange["auto_delete"],
        "durable": exchange["durable"],
        "internal": exchange["internal"],
        "type": exchange["type"],
        "user_who_performed_action": exchange["user_who_performed_action"],
    }
    return payload


def build_queue_payload(queue):
    payload = {
        "arguments": queue["arguments"],
        "auto_delete": queue["auto_delete"],
        "durable": queue["durable"],
        "exclusive": queue["exclusive"],
        "type": queue["type"],
        "vhost": queue["vhost"]
    }
    return payload


def get_queue_list(url, vhost, key_word):
    get_queue_url = f"{url}/queues/{vhost}?page=1&page_size=500&name={key_word}&use_regex=true&pagination=true"
    response = requests.get(get_queue_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
    if not response.ok:
        print(response.text, "Queues could not be gotten")
        return
    existing_queues_dict = response.json()
    print("## QUEUES SUCCESSFULLY READ ##")
    print("#############################QUEUES#############################")
    print("## QUEUE-NAME             ##  VHOST ##################")
    for queue in existing_queues_dict["items"]:
        queue_name = queue["name"]
        vhost_name = queue["vhost"]
        print("##", queue_name, "##", vhost_name, "##")
    print("##############################################################")
    return existing_queues_dict


def get_vhosts_list(url):
    get_vhosts_url = f"{url}/vhosts"
    response = requests.get(get_vhosts_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
    if not response.ok:
        print(response.text, "Vhosts could not be gotten")
        return
    print("## VHOSTS SUCCESSFULLY READ ##")
    existing_vhost_list = response.json()
    print("########################VHOSTS########################")
    for vhost in existing_vhost_list:
        vhost_name = vhost["name"]
        vhosts.append(vhost_name)
        print("##", vhost_name, "##")
    print("##############################################################")
    return existing_vhost_list


def get_exchanges_list(url, vhost, key_word):
    exchanges = []
    get_exchange_url = f"{url}/exchanges/{vhost}?page=1&page_size=100&name={key_word}&use_regex=false&pagination=true"
    response = requests.get(get_exchange_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
    if not response.ok:
        print(response.text, "Exchanges could not be gotten")
        return
    print("## EXCHANGES SUCCESSFULLY READ ##")
    existing_exchanges_dict = response.json()
    print("########################EXCHANGES########################")
    print("## EXCHANGE-NAME ##             ## VHOST ##################")
    for exchange in existing_exchanges_dict["items"]:
        exchange_name = exchange["name"]
        if not exchange_name or exchange_name.startswith("amq."):
            continue
        exchanges.append(exchange)
        vhost_name = exchange["vhost"]
        print("##", exchange_name, " ##            ", vhost_name, "##")
    print("##############################################################")
    return exchanges


def filter(existing, key_word, filter_key):
    return [item for item in existing if key_word in item[filter_key]]


def create_queues(existing_queues, url, target_vhost):
    print("## QUEUES ARE CREATING... ##")
    for queue in existing_queues["items"]:
        queue_name = queue["name"]
        create_queue_url = f"{url}/queues/{target_vhost}/{queue_name}"
        add_queue_response = requests.put(create_queue_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD),
                                          json=build_queue_payload(queue))
        if not add_queue_response.ok:
            print(f"{add_queue_response.text} Queue: {queue_name} could not be created.")
            continue
        print(f"## Queue: {queue_name} successfully created ##")
    print("## All QUEUES SUCCESSFULLY CREATED ##")


def create_exchanges(existing_exchanges, local_url, prod_url, target_vhost, source_vhost, key_word):
    print("## EXCHANGES ARE CREATING... ##")
    for exchange in existing_exchanges["items"]:
        exchange_name = exchange["name"]
        if not exchange_name or exchange_name.startswith("amq."):
            continue
        exchanges_url = f"{local_url}/exchanges/{target_vhost}/{exchange_name}"
        add_response = requests.put(exchanges_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD),
                                    json=build_exchange_payload(exchange))
        if not add_response.ok:
            print(add_response.status_code, exchange_name, "could not be created.")
            continue
        print(f"## Exchange: {exchange_name} has been created ##")
    print("## All EXCHANGES SUCCESSFULLY CREATED ##")
    create_binding(prod_url, local_url, source_vhost, target_vhost, key_word)


def create_binding(prod_url, local_url, source_vhost, target_vhost, key_word):
    get_binding_list_url = f"{prod_url}/bindings/{source_vhost}"
    response = requests.get(get_binding_list_url, auth=HTTPBasicAuth(RABBIT_PROD_USERNAME, RABBIT_PROD_PASSWORD))
    if not response.ok:
        print(response.text, "Bindings could not be gotten")
        return
    existing_binding_list = response.json()
    print("## BINDINGS SUCCESSFULLY READ ##")
    for binding in filter(existing_binding_list, key_word, "source"):
        if binding["source"]:
            source = binding["source"]
            dest = binding["destination"]
            binding_url = f"{local_url}/bindings/{target_vhost}/e/{source}/q/{dest}"
            binding_response = requests.post(binding_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD),
                                             json=build_binding_payload(binding))
            if not binding_response.ok:
                print(binding_response.text, source, "could not be bound.")
                continue
    print("## BINDINGS SUCCESSFULLY CREATED ##")


def create_vhost(url, vhost):
    create_vhost_url = f"{url}/vhosts/{vhost}"
    creating_response = requests.put(create_vhost_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
    if not creating_response.ok:
        print(f"{creating_response.text} vhost: {vhost} could not be created")
        return
    print(f"## vhost: {vhost} successfully created ##")


def delete_vhost(url, vhost):
    delete_vhost_url = f"{url}/vhosts/{vhost}"
    deleting_response = requests.delete(delete_vhost_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
    if not deleting_response.ok:
        print(f"{deleting_response.text} Vhost: {vhost} could not be removed.")
        return
    print(f"## Vhost: {vhost} successfully removed ##")


def delete_all_queues(url, existing_queues, vhost):
    for queue in existing_queues["items"]:
        queue_name = queue["name"]
        delete_queue_url = f"{url}/queues/{vhost}/{queue_name}"
        delete_response = requests.delete(delete_queue_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
        if not delete_response.ok:
            print(f"{delete_response.text} queue: {queue_name} could not be removed.")
            continue
        print(f"## Queue: {queue_name} has been removed ##")


def delete_all_exchanges(url, existing_exchanges, vhost, key_word):
    for exchange in existing_exchanges:
        exchange_name = exchange["name"]
        delete_exchange_url = f"{url}/exchanges/{vhost}/{exchange_name}"
        delete_response = requests.delete(delete_exchange_url, auth=HTTPBasicAuth(RABBIT_USERNAME, RABBIT_PASSWORD))
        if not delete_response.ok:
            print(f"{delete_response.text} exchange: {exchange_name} could not be removed.")
            continue
        print(f"## Exchange: {exchange_name} has been removed ##")


def make_table(existing_dict):
    print("## QUEUE-NAME                VHOST """)
    column = existing_dict["name"], existing_dict["vhost"]
    print(column)


if __name__ == "__main__":
    print("################RABBITMQ-MANAGEMENT-SCRIPT################")
    start()
    print("##########################THE-END#########################")
