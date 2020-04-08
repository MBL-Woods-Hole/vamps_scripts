#!/usr/bin/env python
# -*- coding: utf-8 -*-

import util

from collections import defaultdict
import re


class Sequences:
  def __init__(self, input_file):
    pass

if __name__ == '__main__':
  # ~/BPC/python-scripts$ python upload_dco_metadata_csv_to_vamps.py -f /Users/ashipunova/BPC/vamps-node.js/user_data/vamps2/AnnaSh/metadata-project_DCO_GAI_Bv3v5_AnnaSh_1500930353039.csv

  utils = util.Utils()

  if utils.is_local() == True:
    mysql_utils = util.Mysql_util(host = 'localhost', db = 'vamps2', read_default_group = 'clienthome')
    print("host = 'localhost', db = 'vamps2'")
  else:
    mysql_utils = util.Mysql_util(host = 'vampsdb', db = 'vamps2', read_default_group = 'client')
    # mysql_utils = util.Mysql_util(host = 'vampsdev', db = 'vamps2', read_default_group = 'client')
    # print("host = 'vampsdev', db = 'vamps2'")

  parser = argparse.ArgumentParser()

  parser.add_argument('-f', '--file_name',
                      required = True, action = 'store', dest = 'input_file',
                      help = '''Input file name''')
  parser.add_argument("-ve", "--verbatim",
                      required = False, action = "store_true", dest = "is_verbatim",
                      help = """Print an additional inforamtion""")

  args = parser.parse_args()
  print('args = ')
  print(args)

  is_verbatim = args.is_verbatim

  metadata = Metadata(args.input_file)
  required_metadata = RequiredMetadata()
  custom_metadata = CustomMetadata()

  required_metadata_update = required_metadata.required_metadata_update

  # add as data to custom_metadata_fields for project_id = ## and as columns to custom_metadata_##
  add_fields_to_db_dict = custom_metadata.fields_to_add_to_db
  # print("FFF6 custom_metadata.fields_to_add_to_db = ")
  # print(custom_metadata.fields_to_add_to_db)

  custom_metadata_update = custom_metadata.custom_metadata_update
  project_id = custom_metadata.project_id

  if (is_verbatim):
    print('QQQ1 = required_metadata_update')
    print(required_metadata_update)

    print('QQQ2 = add_fields_to_db_dict')
    print(add_fields_to_db_dict)

    print('QQQ3 = custom_metadata_update')
    print(custom_metadata_update)

  upload_metadata = Upload()
