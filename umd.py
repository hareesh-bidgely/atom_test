import os
import csv
import time
import requests
import json
import argparse
import threading
import pandas as pd
import numpy as np

import functools
print = functools.partial(print, flush=True)

parser = argparse.ArgumentParser()
parser.add_argument("--uuid", help="input uuid")
parser.add_argument("--userlist", help="input file")
parser.add_argument("op_file", help="output file")
parser.add_argument("token", help="token for the url")
parser.add_argument("max_threads", help="max threads count")
parser.add_argument("max_users", help="max users for single thread")

args = parser.parse_args()
max_threads = int(args.max_threads)
max_users = int(args.max_users)
token = args.token
op_file = args.op_file


def create_directory(op_folder):
    if not os.path.isdir(op_folder):
        os.makedirs(op_folder)


def get_list_of_users():
    if args.uuid:
        users = [args.uuid]
    elif args.userlist:
        df_users = pd.read_csv(args.userlist)
        users = list(df_users['UUID'].unique())
        users.sort()

    return users


def get_user_data(uuid, token):
    # url='https://naapi-read.bidgely.com/v2.0/users/{uuid}/?access_token={token}'.format(uuid=uuid,token=token)
    url = 'https://naapi-read.bidgely.com/meta/users/{uuid}/?access_token=1d3284dc-7364-4121-bbc0-110869bd2cba'.format(
        uuid=uuid)
    # url='https://naapi-read.bidgely.com/meta/users/{uuid}/homes/1/gws/3?access_token={token}'.format(uuid=uuid,token=token)
    # print(url)
    response = requests.get(url)
    data = response.json()
    # payload=data["payload"]
    # tags=payload["utilityTags"]['account_and_premise_number'].split(':')
    fname = data["fname"]
    lname = data["lname"]
    email = data["email"]

    # s=uuid+"|["+tags[0]+"]-["+tags[1]+"]["+tags[2]+"\n"
    # s="{}|[{}]-[{}]-[{}]\n".format(uuid,tags[0],tags[1],payload["utilityTags"]['customer_sequence_id'])
    s = f'{uuid}|{fname}|{lname}|{email}\n'
    # print(s)
    return s


def save_data_to_file(file_path, data):
    with open(file_path, "a+") as temp_file:
        temp_file.writelines(data)


def main_function(userlist, token, file_path):
    users_info_list = []
    for uuid in userlist:
        data = get_user_data(uuid, token)
        users_info_list.append(data)

    save_data_to_file(file_path, users_info_list)


def user_lists_splitter():
    all_users_list = get_list_of_users()
    # all_users_list=all_users_list[0:4]
    users_count = len(all_users_list)
    print("Total Users found:" + str(users_count))
    max_user_proccesing_count_per_round = max_threads * max_users

    main_list = []

    for i in range(0, users_count, max_user_proccesing_count_per_round):
        subsets_list = []
        user_subset = all_users_list[i:i + max_user_proccesing_count_per_round]
        for j in range(0, max_user_proccesing_count_per_round, max_users):
            mini_user_list = user_subset[j:j + max_users]
            if len(mini_user_list) > 0:
                subsets_list.append(mini_user_list)

        main_list.append(subsets_list)

    return main_list, users_count


def threads_executor(thread_count, user_sets_list, file_path):
    threads = []
    for j in range(thread_count):
        users_list = user_sets_list[j]
        thread = threading.Thread(
            target=main_function, args=(users_list, token, file_path,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":

    op_folder = 'User_Meta_data_Op_dir'
    create_directory(op_folder)

    file_path = os.getcwd() + '/' + op_folder + '/' + op_file + '.csv'
    if os.path.exists(file_path):
        os.remove(file_path)
        print("removed_file\n" + file_path)

    users_sets_list, users_count = user_lists_splitter()
    max_user_proccesing_count_per_round = max_threads * max_users

    no_of_users = 0
    for users_subsets in users_sets_list:
        thread_count = len(users_subsets)
        print(thread_count)
        threads_executor(thread_count, users_subsets, file_path)
        no_of_users += (thread_count * max_users)
        if no_of_users > users_count:
            print("completed user count: " + str(users_count))
        else:
            print("completed user count: " + str(no_of_user))
