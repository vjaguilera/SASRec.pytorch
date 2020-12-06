import json_lines
import datetime
import numpy as np
import pandas as pd

def check_item_id(record):
  return type(record) == int
    

def convert_set_to_dict(file_dir):
  """Converts a dataset file into user-item-tmps dict"""
  user_item_tmstmp = {
      "user_id": [],
      "item_id": [],
      "timestamp": []
  }

  with json_lines.open(file_dir) as f:
      for index, item in enumerate(f):

        # Save User Events
        for event in item['user_history']:
          if check_item_id(event['event_info']):
            user_item_tmstmp['user_id'].append(index+1)
            user_item_tmstmp['item_id'].append(event['event_info'])
            user_item_tmstmp['timestamp'].append(datetime.datetime.strptime(event['event_timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z'))

        # Save User Item Tmpstmp Bought
        user_item_tmstmp['user_id'].append(index+1)
        user_item_tmstmp['item_id'].append(item['item_bought'])
        user_item_tmstmp['timestamp'].append(user_item_tmstmp['timestamp'][-1] + datetime.timedelta(hours = 2))

  print("Finished reading file, proceding Dataframe")
  return user_item_tmstmp


def convert_item_set_to_dict(file_dir):
  """Converts a dataset file into dict of item-information"""

  item_info = {
      "item_id": [],
      "title": [],
      "price": [],
      "category_id": [],
      "product_id": [],
      "domain_id": [],
      "condition": []
  }

  with json_lines.open(file_dir) as f:
      for item in enumerate(f):
        item_info['item_id'].append(item[1]['item_id'])
        item_info['title'].append(item[1]['title'])
        item_info['price'].append(item[1]['price'])
        item_info['category_id'].append(item[1]['category_id'])
        item_info['product_id'].append(item[1]['product_id'])
        item_info['domain_id'].append(item[1]['domain_id'])
        item_info['condition'].append(item[1]['condition'])

  print("Finished reading file, proceding Dataframe")
  return item_info

def process_events(dst_path, df_user_item_tmstmp):
    # Filter by session size > 1
    # session_lengths = df_user_item_tmstmp.groupby('user_id').size()
    # df_user_item_tmstmp = df_user_item_tmstmp[np.in1d(df_user_item_tmstmp.user_id, session_lengths[session_lengths>1].index)]

    # Filter by "known" (appears >= 5) items
    # item_supports = df_user_item_tmstmp.groupby('item_id').size()
    # df_user_item_tmstmp = df_user_item_tmstmp[np.in1d(df_user_item_tmstmp.item_id, item_supports[item_supports>=5].index)]

    df_user_item = df_user_item_tmstmp[['user_id', 'item_id']]
    
    print('Dataset\n\tEvents: {}\n\tSessions: {}\n\tItems: {}'.format(len(df_user_item), df_user_item.user_id.nunique(), df_user_item.item_id.nunique()))
    df_user_item.to_csv(dst_path + 'meli.txt', sep=' ', index=None, header=None, mode='a')

def process_items(dst_path, df_items):
    print('Dataset\n\tItems: {}\n\tCategories: {}\n\tConditions: {}'.format(len(df_items), df_items.category_id.nunique(), df_items.condition.nunique()))
    df_items.to_csv(str(dst_path) + 'meli_items.txt',
                        sep=';',
                        index=None,
                        header=None,
                        mode='a')

def create_process_file(src_path, dst_path):
    # RATINGS
    print("Creating ratings file")
    ratings_dict  = convert_set_to_dict(src_path + 'train_dataset.jl.gz')
    df_ratings = pd.DataFrame(ratings_dict)
    process_events(dst_path, df_ratings)

    # ITEMS
    print("Creating items file")
    items_dict = convert_item_set_to_dict(src_path + 'item_data.jl.gz')
    df_items = pd.DataFrame(items_dict)
    process_items(dst_path, df_items)

    # USERS
    print("There is no available data from users")
