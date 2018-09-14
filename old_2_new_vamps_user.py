#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2011, Marine Biological Laboratory
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version.
#
# Please read the COPYING file.
#
"""
*) 
t0 = time.time()
t1 = time.time()
total = t1-t0
print("time_res = %s s" % total)


*) cd vamps-node.js/public/scripts/maintenance_scripts; time python old_to_new_vamps_by_project.py -s sequences.csv -m metadata.csv -owner admin -p "ICM_AGW_Bv6"

*) Utils, connection - classes for all

*)
mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_metadata where project = 'DCO_BOM_Bv6';" |sed "s/'/\'/;s/\t/\"\t\"/g;s/^/\"/;s/$/\"/;s/\n//g" > metadata.csv
mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences where project = 'DCO_BOM_Bv6';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > sequences.csv
mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences_pipe where project = 'DCO_BOM_Bv6';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > sequences.csv

*) beforehand
access
classifier
dataset
domain
env_sample_source
family
genus
gg_otu
gg_taxonomy
klass
oligotype
order
phylum
project
rank
# ref_silva_taxonomy_info_per_seq_refhvr_id
# refhvr_id
# sequence
# sequence_pdr_info
# sequence_uniq_info
# silva_taxonomy
# silva_taxonomy_info_per_seq
species
strain
user
user_project
user_project_status

*) from sequences.csv
ref_silva_taxonomy_info_per_seq_refhvr_id
refhvr_id
sequence
sequence_pdr_info
sequence_uniq_info
silva_taxonomy
silva_taxonomy_info_per_seq

*)
from metadata.csv
required_metadata_info
custom_metadata_fields
custom_metadata_#

"""
import csv
import pymysql as MySQLdb
import logging
import sys
import os
import timeit
import time
from collections import defaultdict

class Mysql_util:
    """
    Connection to vamps or vampsdev

    Takes parameters from ~/.my.cnf_node, default host = "vampsdev", db = "vamps2"
    if different use my_conn = Mysql_util(host, db)
    """
    def __init__(self, host = "bpcweb7", db = "vamps2", read_default_file = os.path.expanduser("~/.my.cnf_node"), port = 3306):
        self.utils     = Utils()
        self.conn      = None
        self.cursor    = None
        self.rows      = 0
        self.new_id    = None
        self.lastrowid = None
        self.rowcount  = None
        
        if read_default_file == "":
          if self.utils.is_local():
            read_default_file = os.path.expanduser("~/.my.cnf_local")
          else:
            read_default_file = os.path.expanduser("~/.my.cnf_node")
        print("read_default_file = %s" % read_default_file)

        try:
            self.utils.print_both("=" * 40)
            self.utils.print_both("host = " + str(host) + ", db = "  + str(db))
            self.utils.print_both("=" * 40)

            self.conn = MySQLdb.connect(host = host, db = db, read_default_file = read_default_file, port = port)
            print("host = %s, db = %s, read_default_file = %s" % (host, db, read_default_file))
            
          # else:
          #   self.conn = MySQLdb.connect(host = host, db = db, read_default_file)
            # self.db = MySQLdb.connect(host = "localhost", # your host, usually localhost
            #                          read_default_file = "~/.my.cnf_node"  )
            # cur = db.cursor()
            self.cursor = self.conn.cursor()
            # self.escape = self.conn.escape()

        except MySQLdb.Error, e:
            self.utils.print_both("Error %d: %s" % (e.args[0], e.args[1]))
            raise
        except:                       # catch everything
            self.utils.print_both("Unexpected:")
            self.utils.print_both(sys.exc_info()[0])
            raise                       # re-throw caught exception

    def execute_fetch_select(self, sql):
     
      if self.cursor:
        try:
          self.cursor.execute(sql)
          res         = self.cursor.fetchall ()
          field_names = [i[0] for i in self.cursor.description]
        except:
          self.utils.print_both(("ERROR: query = %s") % sql)
          raise
        return (res, field_names)

    def execute_no_fetch(self, sql):
      if self.cursor:
          self.cursor.execute(sql)
          self.conn.commit()
          return self.cursor.lastrowid

    def execute_insert(self, table_name, field_name, val_list, ignore = "IGNORE"):
      try:
        sql = "INSERT %s INTO %s (%s) VALUES (%s)" % (ignore, table_name, field_name, val_list)
        print('sql',sql)
       
        if self.cursor:
          self.cursor.execute(sql)
          self.conn.commit()
          return (self.cursor.rowcount, self.cursor.lastrowid)
      except:
        self.utils.print_both(("ERROR: query = %s") % sql)
        raise

    def get_all_name_id(self, table_name, id_name = "", field_name = "", where_part = ""):
      if (field_name == ""):
        field_name = table_name
      if (id_name == ""):
        id_name = table_name + '_id'
      my_sql  = """SELECT %s, %s FROM %s %s""" % (field_name, id_name, table_name, where_part)
      # self.utils.print_both(("my_sql from get_all_name_id = %s") % my_sql)
      res     = self.execute_fetch_select(my_sql)
      
      if res:
        return res[0]

    def execute_simple_select(self, field_name, table_name, where_part):
      id_query  = "SELECT %s FROM %s %s" % (field_name, table_name, where_part)
      return self.execute_fetch_select(id_query)[0]

    def get_id(self, field_name, table_name, where_part, rows_affected = [0,0]):
      # self.utils.print_array_w_title(rows_affected, "=====\nrows_affected from def get_id")

      if rows_affected[1] > 0:
        id_result = int(rows_affected[1])
      else:
        try:
          # id_query  = "SELECT %s FROM %s %s" % (field_name, table_name, where_part)
          id_result_full = self.execute_simple_select(field_name, table_name, where_part)
          id_result = int(id_result_full[0][0])
        except:
          self.utils.print_both("Unexpected:")
          # self.utils.print_both(sys.exc_info()[0])
          raise

      # self.utils.print_array_w_title(id_result, "=====\nid_result IN get_id")
      return id_result


class Utils:
    def __init__(self):
        self.chunk_split = 100
        self.min_seqs = 300000
        pass

    def is_local(self):
        print(os.uname()[1])


        dev_comps = ['ashipunova.mbl.edu', "as-macbook.home", "as-macbook.local", "Ashipunova.local", "Annas-MacBook-new.local", "Annas-MacBook.local",'Andrews-Mac-Pro.local']


        if os.uname()[1] in dev_comps:
            return True
        else:
            return False

    def is_vamps(self):
        print(os.uname()[1])
        dev_comps = ['bpcweb8','bpcweb7','bpcweb7.bpcservers.private', 'bpcweb8.bpcservers.private']
        if os.uname()[1] in dev_comps:
            return True
        else:
            return False
            
    def is_vamps_prod(self):
        print(os.uname()[1])
        dev_comps = ['bpcweb8', 'bpcweb8.bpcservers.private']
        if os.uname()[1] in dev_comps:
            return True
        else:
            return False
            
    def print_both(self, message):
        print(message)
        logging.debug(message)

    def print_array_w_title(self, message, title = 'message'):
      print(title)
      print(message)

    def read_csv_into_list(self, file_name):
      csv_file_content_all = list(csv.reader(open(file_name, 'rb'), delimiter = ','))
      try:
        csv_file_fields      = csv_file_content_all[0]
        csv_file_content     = csv_file_content_all[1:]
      except:
        csv_file_fields      = []
        csv_file_content     = []
      return (csv_file_fields, csv_file_content)
      # return list(csv.reader(open(file_name, 'rb'), delimiter = ','))[1:]

    def flatten_2d_list(self, list):
      return [item for sublist in list for item in sublist]

    def wrapper(self, func, *args, **kwargs):
        def wrapped():
            return func(*args, **kwargs)
        return wrapped


    def search_in_2d_list(self, search, data):
      for sublist in data:
        if search in sublist:
          return sublist
          break

    def make_insert_values(self, matrix):
      all_insert_vals = ""

      for arr in matrix[:-1]:
        insert_dat_vals = ', '.join("'%s'" % key for key in arr)
        all_insert_vals += insert_dat_vals + "), ("

      all_insert_vals += ', '.join("'%s'" % key for key in matrix[-1])

      # self.print_array_w_title(all_insert_vals, "all_insert_vals from make_insert_values")
      return all_insert_vals

    def find_val_in_nested_list(self, hey, needle):
      return [v for k, v in hey if k.lower() == needle.lower()]

    def find_key_by_value_in_dict(self, hey, needle):
      return [k for k, v in hey if v == needle]

    def make_entry_w_fields_dict(self, fields, entry):
      return dict(zip(fields, entry))

    def write_to_csv_file(self, file_name, res, file_mode = "wb"):
      data_from_db, field_names = res
      

      with open(file_name, file_mode) as csv_file:
        csv_writer = csv.writer(csv_file)
        if file_mode == "wb":
          csv_writer.writerow(field_names) # write headers
        csv_writer.writerows(data_from_db)    

    def get_csv_file_calls(self, query):
      return prod_mysql_util.execute_fetch_select(query)
      
    def slicedict(self, my_dict, key_list):
      return {k: v for k, v in my_dict.items() if k in key_list}

    def chunks(self, data, n=10):
        return [data[i:i+n] for i in range(0, len(data), n)]

   



class User:
  def __init__(self, username,   new_vamps_mysql, old_vamps_mysql):
    self.utils      = Utils()
    self.user_contact_file_content = []
    self.user_id    = ""
    self.username    = username
    self.new_vamps_mysql = new_vamps_mysql
    self.old_vamps_mysql = old_vamps_mysql

  
  def get_user(self):
    query = """SELECT distinct contact, user as username, email, institution, first_name, last_name, active, security_level, passwd as encrypted_password 
              FROM new_user_contact 
              JOIN new_user using(user_id) 
              JOIN new_contact using(contact_id) 
              WHERE user='%s'""" % (self.username)
    data = self.old_vamps_mysql.execute_fetch_select(query)
    self.user_data = data[0][0]
    print(self.user_data)
    return 1
            
  def parse_user_contact_csv(self, user_contact_csv_file_name):
    self.user_contact_file_content = self.utils.read_csv_into_list(user_contact_csv_file_name)[1]
    # self.utils.print_array_w_title(self.user_contact_file_content, "===\nself.user_contact_file_content BBB")

  def insert_user(self):
    field_list    = "username, email, institution, first_name, last_name, active, security_level, encrypted_password"
    try:
      insert_values = ', '.join(["'%s'" % key for key in self.user_data[1:]])
      self.user = ''
    except:
      """
        If project contact is not in the user database (common for older projects)
            then we need to supply a fallback contact so the script continues
      """
      insert_values = ', '.join(["'admin'","'admin@no-reply.edu'","'MBL'","'Ad'","'Min'","'0'","'50'","'@@@@@@@@@'"])
      self.user = 'admin'
      
      #self.utils.print_both("\n!!!\nPlease check if contact information from project (" + self.contact + ") corresponds with user_contact_PROJECT.csv\n!!!\n")
      #raise
    
    rows_affected = self.new_vamps_mysql.execute_insert("user", field_list, insert_values)
    self.utils.print_array_w_title(rows_affected, "rows affected by insert_user")
    if self.user == 'admin':
         username = 'admin'
    else:
        username = self.user_data[1]
    self.user_id  = self.new_vamps_mysql.get_id("user_id", "user", "WHERE username = '%s'" % (username), rows_affected)
    return self.user_id
    
    
    
  def get_user_id(self):
   username = self.user_data[1]
   self.user_id  = self.new_vamps_mysql.get_id("user_id", "user", "WHERE username = '%s'" % (username))
            
   return  self.user_id
    

class Project:

  def __init__(self, mysql_util):
    self.utils      = Utils()
    self.contact    = ""
    self.project_id = ""
    self.user_id    = ""
    self.project_dict = {}
    self.project    = ""

  def parse_project_csv(self, project_csv_file_name):
    # "project","title","project_description","funding","env_sample_source_id","contact","email","institution"

    self.project_file_content = self.utils.read_csv_into_list(project_csv_file_name)[1]
    # self.utils.print_array_w_title(self.project_file_content, "===\nself.project_file_content AAA")
    self.contact              = self.project_file_content[0][5]
    self.project         = self.project_file_content[0][0]    

  def insert_project(self, user_id):
    project, title, project_description, funding, env_sample_source_id, contact, email, institution = self.project_file_content[0]

    field_list     = "project, title, project_description, rev_project_name, funding, owner_user_id"
    insert_values  = ', '.join("'%s'" % key for key in [project, title, project_description])
    insert_values += ", REVERSE('%s'), '%s', %s" % (project, funding, user_id)

    # sql = "INSERT %s INTO %s (%s) VALUES (%s)" % ("ignore", "project", field_list, insert_values)
    # self.utils.print_array_w_title(sql, "sql")

    rows_affected = mysql_util.execute_insert("project", field_list, insert_values)
    self.utils.print_array_w_title(rows_affected, "rows_affected by insert_project")

    self.project_id = mysql_util.get_id("project_id", "project", "WHERE project = '%s'" % (self.project), rows_affected)
    
    # self.utils.print_array_w_title(self.project_dict, "===\nSSS self.project_dict from insert_project ")
    
  def get_project_id(self):
    self.project_id = mysql_util.get_id("project_id", "project", "WHERE project = '%s'" % self.project)
    
  def make_project_dict(self):
    self.project_dict[self.project] = self.project_id    



if __name__ == '__main__':
  import subprocess
  import argparse
  
  parser = argparse.ArgumentParser(description = "")

  
  parser.add_argument("-w","--write_files",
      required = False, action = "store_true", dest = "write_files",
      help = """Create csv files first""")
  parser.add_argument("-ni","--do_not_insert",
      required = False, action = "store_true", dest = "do_not_insert", default = False,
      help = """Do not insert data into db, mostly for debugging purposes""")
  parser.add_argument("-user","--user",
      required = True, action = "store", dest = "username", default = '',
      help = """Username""")    
  parser.add_argument("-site", "--site",
        required = True, action = "store", dest = "site", 
        help = """Site where the script is running""")
  if len(sys.argv[1:]) == 0:
        print(myusage)
        sys.exit() 
  args = parser.parse_args()
  
  utils = Utils()
  
  print("args = ")
  print(args)
  print("args.write_files")
  print(args.write_files)

  host_prod   = "vampsdev"
  to_database = 'vamps2'
  if args.site == 'vamps':
      host_prod = "vampsdb"
  else:
      host_prod = "vampsdev"
  port_prod = 3306
 

  
  user_contact_csv_file_name = "user_contact_%s.csv" % (args.username)
 
  

  # ========

  #print("metadata_csv_file_name = %s, seq_csv_file_name = %s, project_csv_file_name = %s, dataset_csv_file_name = %s, user_contact_csv_file_name = %s" % (metadata_csv_file_name, seq_csv_file_name, project_csv_file_name, dataset_csv_file_name, user_contact_csv_file_name))
  old_vamps_mysql = Mysql_util(host = host_prod, db = "vamps", read_default_file = "~/.my.cnf", port = port_prod)
  new_vamps_mysql = Mysql_util(host = host_prod, db = 'vamps2')
  
 
  user = User(args.username, new_vamps_mysql, old_vamps_mysql)
  user.get_user()
  
  if (args.do_not_insert == False):
    user.insert_user()
  print(user.get_user_id())
  
  sys.exit()
  
  
 