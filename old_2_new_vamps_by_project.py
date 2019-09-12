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
import os,math
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
            
            # cur = db.cursor()
            self.cursor = self.conn.cursor()
            # self.escape = self.conn.escape()

        except MySQLdb.Error as e:
            self.utils.print_both("Error %d: %s" % (e.args[0], e.args[1]))
            raise
        except:                       # catch everything
            self.utils.print_both("1-Unexpected:")
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
        #if table_name == 'sequence_uniq_info' or table_name == 'silva_taxonomy_info_per_seq':
        #  DO NOT UPDATE USER ONLY ADD
        if (args.do_not_update == False and table_name != 'user'):
            sql += " ON DUPLICATE KEY UPDATE\n"   # IGNORE keyword ignored when this is used
            for field in field_name.split(","):
                field = field.strip()
                sql += field+"=VALUES("+field+"),"
            sql = sql.rstrip(',') # remove last comma        
        
        #if table_name == 'dataset' or table_name == 'project':
       
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
      #print('RES')
      #print(res[0])
      if res:
        return res[0]   

    def execute_simple_select(self, field_name, table_name, where_part):
      id_query  = "SELECT %s FROM %s %s" % (field_name, table_name, where_part)
      #print(id_query)
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
          self.utils.print_both("FAILED: self.execute_simple_select("+field_name+", "+table_name+", "+where_part+")")
          # self.utils.print_both(sys.exc_info()[0])
          id_result = 0
          #raise

      # self.utils.print_array_w_title(id_result, "=====\nid_result IN get_id")
      return id_result


class Utils:
    def __init__(self):
        self.chunk_split = 100
        self.min_seqs = 10000 #50000  #300000
        pass

    def is_local(self):
        print('xx',os.uname()[1])

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
      fh = open(file_name, 'r', encoding='utf-8', newline='')
      yy = csv.reader(fh, delimiter = ',')
      
      csv_file_content_all = list(yy)
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

    def benchmarking(self, func, func_name, *args, **kwargs):
      print("START %s" % func_name)
      wrapped  = utils.wrapper(func, *args)
      time_res = timeit.timeit(wrapped, number = 1)
      print('time: %.2f s' % time_res)
      

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
      return dict(list(zip(fields, entry)))

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

class CSV_files:
  def __init__(self):
    pass
  
  def run_md_dump(self, prod_mysql_util):
    project = args.project
    query = "SELECT * FROM vamps_metadata where project = '%s'" % (project)  
    metadata_csv_file_name = "metadata_%s.csv" % project
    utils.write_to_csv_file(metadata_csv_file_name, utils.get_csv_file_calls(query))
    return metadata_csv_file_name
    
  def run_csv_dump(self, prod_mysql_util):
    # TODO: add directory from args
    project = args.project
    metadata_csv_file_name = self.run_md_dump(prod_mysql_util)

    query = "SELECT * FROM vamps_sequences where project = '%s'" % (project)  
    seq_csv_file_name = "sequences_%s.csv" % project
    utils.write_to_csv_file(seq_csv_file_name, utils.get_csv_file_calls(query))

    query = "SELECT * FROM vamps_sequences_pipe where project = '%s'" % (project)  
    seq_csv_file_name = "sequences_%s.csv" % project
    utils.write_to_csv_file(seq_csv_file_name, utils.get_csv_file_calls(query), "ab")

    query = """SELECT DISTINCT project, title, project_description, funding, env_sample_source_id, contact, email, institution 
                FROM new_project 
                LEFT JOIN new_contact using(contact_id) 
                WHERE project = '%s' 
               UNION 
               SELECT project_name AS project, title, description AS project_description, 0 AS funding, env_source_id AS env_sample_source_id, contact, email, institution 
                FROM vamps_upload_info 
                WHERE project_name = '%s'""" % (project, project)  
    project_csv_file_name = "project_%s.csv" % project
    utils.write_to_csv_file(project_csv_file_name, utils.get_csv_file_calls(query))

    query = """SELECT distinct contact, user as username, email, institution, first_name, last_name, active, security_level, passwd as encrypted_password 
              FROM new_user_contact 
              JOIN new_user using(user_id) 
              JOIN new_contact using(contact_id) 
              WHERE first_name is not NULL and first_name <> '';"""
    user_contact_csv_file_name = "user_contact_%s.csv" % project
    utils.write_to_csv_file(user_contact_csv_file_name, utils.get_csv_file_calls(query))

    query = """SELECT DISTINCT dataset, dataset_description, env_sample_source_id, project 
                  FROM new_dataset 
                  JOIN new_project using(project_id) 
                  WHERE project = '%s' 
                UNION 
                SELECT DISTINCT dataset, dataset_info AS dataset_description, env_source_id AS env_sample_source_id, project 
                  FROM vamps_projects_datasets_pipe 
                  JOIN vamps_upload_info ON(project = project) 
                  WHERE project = '%s'
          ;"""  % (project, project)
    dataset_csv_file_name = "dataset_%s.csv" % project
    utils.write_to_csv_file(dataset_csv_file_name, utils.get_csv_file_calls(query))
    return (metadata_csv_file_name, seq_csv_file_name, project_csv_file_name, dataset_csv_file_name, user_contact_csv_file_name)
  
    

class Taxonomy:
  def __init__(self, taxa_content, mysql_util):
    self.utils        = Utils()
    self.taxa_content = taxa_content
    self.ranks        = ['domain', 'phylum', 'klass', 'order', 'family', 'genus', 'species', 'strain']
    self.taxa_by_rank = []
    self.all_rank_w_id                       = set()
    self.uniqued_taxa_by_rank_dict           = {}
    self.uniqued_taxa_by_rank_w_id_dict      = {}
    self.taxa_list_w_empty_ranks_dict        = defaultdict(list)
    self.taxa_list_w_empty_ranks_ids_dict    = defaultdict(list)
    self.silva_taxonomy_rank_list_w_ids_dict = defaultdict(list)
    self.silva_taxonomy_ids_dict             = defaultdict(list)
    self.silva_taxonomy_id_per_taxonomy_dict = defaultdict(list)

  def parse_taxonomy(self):
    self.taxa_list_dict = {taxon_string: taxon_string.split(";") for taxon_string in self.taxa_content}
    self.taxa_list_w_empty_ranks_dict = {taxonomy: tax_list + [""] * (len(self.ranks) - len(tax_list)) for taxonomy, tax_list in self.taxa_list_dict.items()}

  def get_taxa_by_rank(self):
    self.taxa_by_rank = list(zip(*self.taxa_list_w_empty_ranks_dict.values()))

  def make_uniqued_taxa_by_rank_dict(self):
    for rank in self.ranks:
      rank_num             = self.ranks.index(rank)
      uniqued_taxa_by_rank = set(self.taxa_by_rank[rank_num])
      try:
        self.uniqued_taxa_by_rank_dict[rank] = uniqued_taxa_by_rank
      except:
        raise

    # self.utils.print_array_w_title(self.uniqued_taxa_by_rank_dict, "self.uniqued_taxa_by_rank_dict made with for")

  def insert_taxa(self):
    """
    TODO: make all queries, then insert all? Benchmark!
    """
    for rank, uniqued_taxa_by_rank in self.uniqued_taxa_by_rank_dict.items():
      insert_taxa_vals = '), ('.join(["'%s'" % key for key in uniqued_taxa_by_rank])

      shielded_rank_name = self.shield_rank_name(rank)
      rows_affected = mysql_util.execute_insert(shielded_rank_name, shielded_rank_name, insert_taxa_vals)
      self.utils.print_array_w_title(rows_affected, "rows affected by mysql_util.execute_insert(%s, %s, insert_taxa_vals)" % (rank, rank))

  def shield_rank_name(self, rank):
    return "`"+rank+"`"

    """
    >>> obj1 = (6, 1, 2, 6, 3)
    >>> obj2 = list(obj1) #Convert to list
    >>> obj2.append(8)
    >>> print(obj2)
    [6, 1, 2, 6, 3, 8]
    >>> obj1 = tuple(obj2) #Convert back to tuple
    >>> print(obj1)
    (6, 1, 2, 6, 3, 8)

    """

  def get_all_rank_w_id(self):
    all_rank_w_id = mysql_util.get_all_name_id("rank")
    klass_id = self.utils.find_val_in_nested_list(all_rank_w_id, "klass")
    t = ("class", klass_id[0])
    l = list(all_rank_w_id)
    l.append(t)
    self.all_rank_w_id = set(l)
    # self.utils.print_array_w_title(self.all_rank_w_id, "self.all_rank_w_id from get_all_rank_w_id")
    # (('domain', 78), ('family', 82), ('genus', 83), ('klass', 80), ('NA', 87), ('order', 81), ('phylum', 79), ('species', 84), ('strain', 85), ('superkingdom', 86))


  def make_uniqued_taxa_by_rank_w_id_dict(self):
    # self.utils.print_array_w_title(self.uniqued_taxa_by_rank_dict, "===\nself.uniqued_taxa_by_rank_dict from def silva_taxonomy")

    for rank, uniqued_taxa_by_rank in self.uniqued_taxa_by_rank_dict.items():
      shielded_rank_name = self.shield_rank_name(rank)
      taxa_names         = ', '.join(["'%s'" % key for key in uniqued_taxa_by_rank])
      taxa_w_id          = mysql_util.get_all_name_id(shielded_rank_name, rank + "_id", shielded_rank_name, 'WHERE %s in (%s)' % (shielded_rank_name, taxa_names))
      self.uniqued_taxa_by_rank_w_id_dict[rank] = taxa_w_id

  def insert_silva_taxonomy(self):

    # self.utils.print_array_w_title(self.taxa_list_w_empty_ranks_ids_dict.values(), "===\nself.taxa_list_w_empty_ranks_ids_dict from def insert_silva_taxonomy")

    field_list = "domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id"
    all_insert_st_vals = self.utils.make_insert_values(list(self.taxa_list_w_empty_ranks_ids_dict.values()))
    rows_affected = mysql_util.execute_insert("silva_taxonomy", field_list, all_insert_st_vals)
    self.utils.print_array_w_title(rows_affected, "rows_affected by inserting silva_taxonomy")

  def silva_taxonomy(self):
    # silva_taxonomy (domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id)
    self.make_uniqued_taxa_by_rank_w_id_dict()
    silva_taxonomy_list = []

    for taxonomy, tax_list in self.taxa_list_w_empty_ranks_dict.items():
      # ['Bacteria', 'Proteobacteria', 'Deltaproteobacteria', 'Desulfobacterales', 'Nitrospinaceae', 'Nitrospina', '', '']
      silva_taxonomy_sublist = []
      for rank_num, taxon in enumerate(tax_list):
        #print('rank_num',rank_num)
        #print('tax_list',tax_list)
        rank     = self.ranks[rank_num]
        taxon_id = int(self.utils.find_val_in_nested_list(self.uniqued_taxa_by_rank_w_id_dict[rank], taxon)[0])
        silva_taxonomy_sublist.append(taxon_id)
        # self.utils.print_array_w_title(silva_taxonomy_sublist, "===\nsilva_taxonomy_sublist from def silva_taxonomy: ")
      self.taxa_list_w_empty_ranks_ids_dict[taxonomy] = silva_taxonomy_sublist
    # self.utils.print_array_w_title(self.taxa_list_w_empty_ranks_ids_dict, "===\ntaxa_list_w_empty_ranks_ids_dict from def silva_taxonomy: ")

  def make_silva_taxonomy_rank_list_w_ids_dict(self):
    for taxonomy, silva_taxonomy_id_list in self.taxa_list_w_empty_ranks_ids_dict.items():
      rank_w_id_list = []
      for rank_num, taxon_id in enumerate(silva_taxonomy_id_list):
        rank = self.ranks[rank_num]
        t = (rank, taxon_id)
        rank_w_id_list.append(t)

      self.silva_taxonomy_rank_list_w_ids_dict[taxonomy] = rank_w_id_list
    # self.utils.print_array_w_title(self.silva_taxonomy_rank_list_w_ids_dict, "===\nsilva_taxonomy_rank_list_w_ids_dict from def make_silva_taxonomy_rank_list_w_ids_dict: ")
    """
    {'Bacteria;Proteobacteria;Alphaproteobacteria;Rhizobiales;Rhodobiaceae;Rhodobium': [('domain', 2), ('phylum', 2016066), ('klass', 2085666), ('order', 2252460), ('family', 2293035), ('genus', 2303053), ('species', 1), ('strain', 2148217)], ...
    """

  def make_rank_name_id_t_id_str(self, rank_w_id_list):
    a = ""
    for t in rank_w_id_list[:-1]:
      a += t[0] + "_id = " + str(t[1]) + " AND\n"
    a += rank_w_id_list[-1][0] + "_id = " + str(rank_w_id_list[-1][1]) + "\n"
    return a

  def make_silva_taxonomy_ids_dict(self, silva_taxonomy_ids):
    for ids in silva_taxonomy_ids:
      self.silva_taxonomy_ids_dict[int(ids[0])] = [int(id) for id in ids[1:]]
    # self.utils.print_array_w_title(self.silva_taxonomy_ids_dict, "===\nsilva_taxonomy_ids_dict from def get_silva_taxonomy_ids: ")
    # {2436595: [2, 2016066, 2085666, 2252460, 2293035, 2303053, 1, 2148217], 2436596: [...

  def get_silva_taxonomy_ids(self):
    self.make_silva_taxonomy_rank_list_w_ids_dict()

    sql_part = ""
    for taxonomy, rank_w_id_list in list(self.silva_taxonomy_rank_list_w_ids_dict.items())[:-1]:
      a = self.make_rank_name_id_t_id_str(rank_w_id_list)
      sql_part += "(%s) OR " % a

    a_last = self.make_rank_name_id_t_id_str(list(self.silva_taxonomy_rank_list_w_ids_dict.values())[-1])
    sql_part += "(%s)" % a_last

    field_names = "silva_taxonomy_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id"
    table_name  = "silva_taxonomy"
    where_part  = "WHERE " + sql_part
    silva_taxonomy_ids = mysql_util.execute_simple_select(field_names, table_name, where_part)

    """
    ((2436595L, 2L, 2016066L, 2085666L, 2252460L, 2293035L, 2303053L, 1L, 2148217L), ...
    """
    self.make_silva_taxonomy_ids_dict(silva_taxonomy_ids)

  def make_silva_taxonomy_id_per_taxonomy_dict(self):
    for silva_taxonomy_id, st_id_list1 in self.silva_taxonomy_ids_dict.items():
      taxon_string = self.utils.find_key_by_value_in_dict(self.taxa_list_w_empty_ranks_ids_dict.items(), st_id_list1)
      self.silva_taxonomy_id_per_taxonomy_dict[taxon_string[0]] = silva_taxonomy_id
    # self.utils.print_array_w_title(self.silva_taxonomy_id_per_taxonomy_dict, "silva_taxonomy_id_per_taxonomy_dict from silva_taxonomy_info_per_seq = ")

class Refhvr_id:

  def __init__(self, refhvr_id, mysql_util):
    self.utils      = Utils()
    self.refhvr_id  = refhvr_id

    self.all_refhvr_id   = set()
    self.refhvr_id_lists = []

  def parse_refhvr_id(self):
    for r_id in self.refhvr_id:
      refhvr_id_list = r_id.split()
      self.refhvr_id_lists.append(refhvr_id_list)

    # self.refhvr_id_lists = [r_id.split() for r_id in self.refhvr_id]
    #slightly slower

    self.all_refhvr_id = set(self.utils.flatten_2d_list(self.refhvr_id_lists))

  def insert_refhvr_id(self):
    insert_refhvr_id_vals = '), ('.join(["'%s'" % key for key in self.all_refhvr_id])
    # self.utils.print_array_w_title(insert_refhvr_id_vals, "===\ninsert_refhvr_id_vals")
    rows_affected = mysql_util.execute_insert("refhvr_id", "refhvr_id", insert_refhvr_id_vals)
    self.utils.print_array_w_title(rows_affected, "rows affected by mysql_util.execute_insert(refhvr_id, refhvr_id, insert_refhvr_id_vals)")

class User:
  def __init__(self, contact, user_contact_csv_file_name, mysql_util):
    self.utils      = Utils()
    self.user_contact_file_content = []
    self.user_id    = ""
    self.contact    = contact
    self.user_contact_csv_file_name = user_contact_csv_file_name

    self.parse_user_contact_csv(self.user_contact_csv_file_name)
    self.user_data = self.utils.search_in_2d_list(self.contact, self.user_contact_file_content)

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
    
    rows_affected = mysql_util.execute_insert("user", field_list, insert_values)
    self.utils.print_array_w_title(rows_affected, "rows affected by insert_user")
    if self.user == 'admin':
         username = 'admin'
    else:
        username = self.user_data[1]
    self.user_id  = mysql_util.get_id("user_id", "user", "WHERE username = '%s'" % (username), rows_affected)
    
    
    
    
  def get_user_id(self):
    try:
      
        if self.user == 'admin':
            username = 'admin'
        else:
            username = self.user_data[1]
        self.user_id  = mysql_util.get_id("user_id", "user", "WHERE username = '%s'" % (username))
            
    except:
      self.utils.print_both("\n!!!\nPlease check if insert_user was successful\n!!!\n")
      raise
    

class Project:

  def __init__(self, mysql_util):
    self.project    = args.project
    pid= self.get_project_id()
    if pid:
        print('This project is already in database -- you must delete it first if you want to update; pid=',pid)
        sys.exit()
    else:
        print('project okay')
    
    self.utils      = Utils()
    self.contact    = ""
    self.project_id = ""
    self.user_id    = ""
    self.project_dict = {}
    self.project    = ""
    self.public     = args.public
    
    
    
  def parse_project_csv(self, project_csv_file_name):
    # "project","title","project_description","funding","env_sample_source_id","contact","email","institution"

    self.project_file_content = self.utils.read_csv_into_list(project_csv_file_name)[1]
    # self.utils.print_array_w_title(self.project_file_content, "===\nself.project_file_content AAA")
    self.contact              = self.project_file_content[0][5]
    self.project         = self.project_file_content[0][0]    

  def insert_project(self, user_id):
    project, title, project_description, funding, env_sample_source_id, contact, email, institution = self.project_file_content[0]

    field_list     = "project, title, project_description, rev_project_name, funding, owner_user_id, public, active, user_project"
    insert_values  = ', '.join("'%s'" % key for key in [project, title, project_description])
    insert_values += ", REVERSE('%s'), '%s', '%s', '%s', '%s', '%s'" % (project, funding, user_id, self.public,'1','1')

    # sql = "INSERT %s INTO %s (%s) VALUES (%s)" % ("ignore", "project", field_list, insert_values)
    # self.utils.print_array_w_title(sql, "sql")

    rows_affected = mysql_util.execute_insert("project", field_list, insert_values)
    self.utils.print_array_w_title(rows_affected, "rows_affected by insert_project")

    self.project_id = mysql_util.get_id("project_id", "project", "WHERE project = '%s'" % (self.project), rows_affected)
    
    # self.utils.print_array_w_title(self.project_dict, "===\nSSS self.project_dict from insert_project ")
    
  def get_project_id(self):
    self.project_id = mysql_util.get_id("project_id", "project", "WHERE project = '%s'" % self.project)
    return self.project_id
    
  def make_project_dict(self):
    self.project_dict[self.project] = self.project_id 
  

class Dataset:
  def __init__(self, mysql_util):
    self.utils                     = Utils()
    self.dataset_project_dict      = {}
    self.dataset_file_content      = []
    self.dataset_id_by_name_dict   = {}
    self.all_dataset_id_by_project_dict = defaultdict(list)

  def make_dataset_project_dictionary(self):
    self.dataset_project_dict = {val[0]: val[2] for val in self.dataset_file_content}

  def parse_dataset_csv(self, dataset_csv_file_name):
  # "dataset","dataset_description","env_sample_source_id","project"

    self.dataset_file_content = self.utils.read_csv_into_list(dataset_csv_file_name)[1]
    self.dataset_file_content = [[p[0],p[1],p[3]] for p in self.dataset_file_content]
    #print(self.dataset_file_content)
    # need to remove field 2 from each row
    # self.utils.print_array_w_title(self.dataset_file_content, "===\nself.dataset_file_content AAA")

  def put_project_id_into_dataset_file_content(self, project_id):
    for dl in self.dataset_file_content:
      dl[2] = project_id

  
  def collect_dataset_ids(self, project_id):
      for dataset, project in self.dataset_project_dict.items():
        dataset_id = mysql_util.get_id("dataset_id", "dataset", "WHERE dataset = '%s' and project_id = %s" % (dataset, project_id))
        self.dataset_id_by_name_dict[dataset] = dataset_id
    
 #  def collect_dataset_ids(self):
#     for dataset, project in self.dataset_project_dict.items():
#       dataset_id = mysql_util.get_id("dataset_id", "dataset", "WHERE dataset = '%s'" % (dataset))
#       self.dataset_id_by_name_dict[dataset] = dataset_id

  def insert_dataset(self, project_dict):
   
    for project in set(list(self.dataset_project_dict.values())):
      project_id = project_dict[project]
      self.put_project_id_into_dataset_file_content(project_id)

      field_list = "dataset, dataset_description, project_id"

      all_insert_dat_vals = self.utils.make_insert_values(self.dataset_file_content)
      # sql = "INSERT %s INTO `%s` (`%s`) VALUES (%s)" % ("ignore", "dataset", field_list, all_insert_dat_vals)
      # self.utils.print_array_w_title(sql, "sql")

      rows_affected = mysql_util.execute_insert("dataset", field_list, all_insert_dat_vals)
      self.utils.print_array_w_title(rows_affected, "rows_affected by insert_dataset")

  def make_all_dataset_id_by_project_dict(self):
    for dat, proj in sorted(self.dataset_project_dict.items()):
        self.all_dataset_id_by_project_dict[proj].append(self.dataset_id_by_name_dict[dat])
    #print("all_dataset_id_by_project_dict")
    
    # {'ICM_SMS_Bv6': [1062, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077]})

  def add_dataset_id_to_list(self, some_list, project):
    return [([dataset_id] + some_list) for dataset_id in self.all_dataset_id_by_project_dict[project]]



class Sequence:

  def __init__(self, sequences, mysql_util):
    self.utils      = Utils()
    self.sequences  = sequences

    self.all_sequences   = set()
    self.sequences_lists = [] 
    self.comp_seq        = "COMPRESS(%s)" % ')), (COMPRESS('.join(["'%s'" % key for key in self.sequences])
    self.sequences_w_ids = set()
    
    
  def get_seq_ids(self):
    ''' AAV -- for some large projects the get/insert command was too long and it crashed
        I break the command up here into chunks. I use this technique 5 times -- maybe make a function??
    '''
    if len(self.sequences) > self.utils.min_seqs:
        sequences_w_ids = []
        split = len(self.sequences)/self.utils.chunk_split  # how many pieces
        for i,seqs in enumerate(self.utils.chunks(self.sequences, int(math.ceil(split)))):
            print(i+1,'/',self.utils.chunk_split,' -- len seqs chunk:',len(seqs))
            comp_seq = "COMPRESS(%s)" % '), COMPRESS('.join(["'%s'" % key for key in seqs])
            sequences_w_ids.extend( mysql_util.get_all_name_id('sequence', '', 'CONVERT(UNCOMPRESS(sequence_comp) USING utf8)', 'WHERE sequence_comp in (%s)' % comp_seq) )
        
        self.sequences_w_ids = sequences_w_ids
    else:
        self.comp_seq = "COMPRESS(%s)" % '), COMPRESS('.join(["'%s'" % key for key in self.sequences])
        self.sequences_w_ids = mysql_util.get_all_name_id('sequence', '', "CONVERT(UNCOMPRESS(sequence_comp) USING 'utf8')", 'WHERE sequence_comp in (%s)' % self.comp_seq)
    #print('2-sequences_w_ids[:10]')
    #print(self.sequences_w_ids[:10])
    # self.utils.print_array_w_title(sequences_w_ids, "sequences_w_ids from get_seq_ids")

  def insert_seq(self):
    print('type self.sequences:',type(self.sequences))
    print('len self.sequences:',len(self.sequences)  )
    
    if len(self.sequences) > self.utils.min_seqs:
        split = len(self.sequences)/self.utils.chunk_split  # how many pieces
        print('split',int(math.ceil(split)))
        for i,seqs in enumerate(self.utils.chunks(self.sequences, int(math.ceil(split)))):
            print(i+1,'/',self.utils.chunk_split,' -- len seq chunk:',len(seqs))
            comp_seq = "COMPRESS(%s)" % ')), (COMPRESS('.join(["'%s'" % key for key in seqs])
            rows_affected = mysql_util.execute_insert("sequence", "sequence_comp", comp_seq)
            self.utils.print_array_w_title(rows_affected, "rows affected by mysql_util.execute_insert(sequence, sequence_comp, comp_seq)")
    else:
        rows_affected = mysql_util.execute_insert("sequence", "sequence_comp", self.comp_seq)
        self.utils.print_array_w_title(rows_affected, "rows affected by mysql_util.execute_insert(sequence, sequence_comp, comp_seq)")

class Seq_csv:
  # id, sequence, project, dataset, taxonomy, refhvr_id, rank, seq_count, frequency, distance, rep_id, project_dataset
  # parse
  # upload
  """
  TODO:
    *) get host and db dynamically, from args
    done) make one connection, in main?
  """

  def __init__(self, seq_csv_file_name, mysql_util):
    self.utils = Utils()
    
    self.seq_csv_file_fields, self.seqs_file_content = self.utils.read_csv_into_list(seq_csv_file_name)
    
    self.content_by_field = self.content_matrix_transposition()
    
    self.sequences        = self.content_by_field[1]
    self.taxa             = self.content_by_field[4]
    self.refhvr_id        = self.content_by_field[5]
    self.the_rest         = self.content_by_field[6:]

    # self.sequence         = Sequence(self.sequences, mysql_util)
    self.sequence_pdr_info_content = []
    self.silva_taxonomy_info_per_seq_list = []
    self.seq_id_w_silva_taxonomy_info_per_seq_id = ()
    self.sequence_uniq_info_values = ""

    
    """
    [['278176', 'TGGACTTGACATGCACTTGTAAGCCATAGAGATATGGCCCCTCTTCGGAGC', 'ICM_SMS_Bv6', 'SMS_0001_2007_09_19', 'Bacteria;Proteobacteria;Deltaproteobacteria;Desulfobacterales;Nitrospinaceae;Nitrospina', 'v6_DU318 v6_DU349 v6_DU400 v6_DU416', 'genus', '2', '0.000136008160489629', '0.03900', 'FL6XCJ201ALT42', 'ICM_SMS_Bv6--SMS_0001_2007_09_19']...]
    """

  def content_matrix_transposition(self):    
    return list(zip(*self.seqs_file_content))

  # def make_seq_list(self):
  #   self.seq_list = [val[1] for val in self.seqs_file_content]

  def get_default_run_info_ill_id(self):
    # select run_info_ill_id from run_info_ill
#     where run_key_id = (select run_key_id from run_key where run_key = "unknown")
#     and run_id = (select run_id from run where run = "unknown")
#     and dataset_id = (select dataset_id from dataset where dataset = "default_dataset")
#     and dna_region_id = (select dna_region_id from dna_region where dna_region = "unknown")
#     and primer_suite_id = (select primer_suite_id from primer_suite where primer_suite = "unknown")
#     and illumina_index_id = (select illumina_index_id from illumina_index where illumina_index = "unknown")
    
    table_name = 'run_info_ill'
    field_name = 'run_info_ill_id'
    where_part = 'where run_key_id = (select run_key_id from run_key where run_key = "unknown")'
    where_part += ' and run_id = (select run_id from run where run = "unknown")'
    where_part += ' and dataset_id = (select dataset_id from dataset where dataset = "default_dataset")'
    where_part += ' and dna_region_id = (select dna_region_id from dna_region where dna_region = "unknown")'
    where_part += ' and primer_suite_id = (select primer_suite_id from primer_suite where primer_suite = "unknown")'
    where_part += ' and illumina_index_id = (select illumina_index_id from illumina_index where illumina_index = "unknown")'
    result = mysql_util.execute_simple_select(field_name, table_name, where_part)
    return result[0][0]
  
  def make_sequence_pdr_info_content(self, dataset_dict):
    print('self.seq_ids_by_name_dict')
    #print(len(self.seq_ids_by_name_dict))
    #print(dict(list(self.seq_ids_by_name_dict.items())[0:2] ))
    #for a in self.seq_ids_by_name_dict:
    #    print('a')
    #    print(a)
    # AAV: this script will always use 'default' run_info_ill_id
    default_run_info_ill_id = self.get_default_run_info_ill_id()
    
    
    for e in self.seqs_file_content:
      temp_tuple = []
      #print(e[1])
      #print(self.seq_ids_by_name_dict[e[1]])
      #if e[1] in self.seq_ids_by_name_dict:
      temp_tuple.append(int(dataset_dict[e[3]]))
      temp_tuple.append(int(self.seq_ids_by_name_dict[e[1]]))
      temp_tuple.append(int(e[7]))
      temp_tuple.append(int(2))
      temp_tuple.append(int(default_run_info_ill_id))
      # self.utils.print_array_w_title(temp_tuple, "temp_tuple AFTER = ")
      self.sequence_pdr_info_content.append(temp_tuple)

  def insert_sequence_pdr_info(self):
    fields = "dataset_id, sequence_id, seq_count, classifier_id, run_info_ill_id"
    
    # self.utils.print_array_w_title(insert_seq_pdr_vals, "insert_seq_pdr_vals")
    
    
    if len(self.sequence_pdr_info_content) > self.utils.min_seqs:
        
        split = len(self.sequence_pdr_info_content)/self.utils.chunk_split  # how many pieces
        for i,vals in enumerate(self.utils.chunks(self.sequence_pdr_info_content, int(math.ceil(split)))):
            insert_seq_pdr_vals = self.utils.make_insert_values(vals)
            print(i+1,'/',self.utils.chunk_split,' -- len vals chunk:',len(insert_seq_pdr_vals) )
            rows_affected = mysql_util.execute_insert('sequence_pdr_info', fields, insert_seq_pdr_vals)
            self.utils.print_array_w_title(rows_affected, "rows_affected by insert_seq_pdr_vals")
    else:
        insert_seq_pdr_vals = self.utils.make_insert_values(self.sequence_pdr_info_content)
        rows_affected = mysql_util.execute_insert('sequence_pdr_info', fields, insert_seq_pdr_vals)
        self.utils.print_array_w_title(rows_affected, "rows_affected by insert_seq_pdr_vals")

  def sequence_pdr_info(self, dataset_dict, sequences_w_ids):
    # (dataset_id, sequence_id, seq_count, classifier_id)
    # classifier_id = 2 GAST  SILVA108_FULL_LENGTH
    # self.sequence.get_seq_ids()
    # sequences_w_ids
    # self.utils.print_array_w_title(sequences_w_ids, "self.seq_ids_by_name_dict = ")
    #print('sequences_w_ids[0:3]')
    #print(sequences_w_ids[0:3])
    self.seq_ids_by_name_dict = dict(sequences_w_ids)
    
    # self.utils.print_array_w_title(self.seq_ids_by_name_dict, "self.seq_ids_by_name_dict = ")
    self.make_sequence_pdr_info_content(dataset_dict)

# ! silva_taxonomy_info_per_seq (sequence_id, silva_taxonomy_id, gast_distance, refssu_id, refssu_count, rank_id)
  def silva_taxonomy_info_per_seq_from_csv(self, taxonomy):
    # TODO: refactoring (too long)
    sequence_id       = 0
    silva_taxonomy_id = 0
    gast_distance     = 0.0
    refssu_id         = 0
    refssu_count      = 0
    rank_id           = 0

    # self.utils.print_array_w_title(taxonomy.silva_taxonomy_id_per_taxonomy_dict, "taxonomy.taxonomy.silva_taxonomy_id_per_taxonomy_dict from silva_taxonomy_info_per_seq = ")

    # self.utils.print_array_w_title(self.seq_ids_by_name_dict, "\n---\nself.seq_ids_by_name_dict from silva_taxonomy_info_per_seq = ")
    # print("SSSSS")
    # seq_csv_file_fields = ["id","sequence","project","dataset","taxonomy","refhvr_ids","rank","seq_count","frequency","distance","rep_id","project_dataset"]
    # all_rank_w_id
    # (('domain', 78), ('family', 82), ('genus', 83), ('klass', 80), ('NA', 87), ('order', 81), ('phylum', 79), ('species', 84), ('strain', 85), ('superkingdom', 86))
    silva_taxonomy_info_per_seq_list1 = []
    for entry in self.seqs_file_content:
      temp_list = []
      entry_w_fields_dict = self.utils.make_entry_w_fields_dict(self.seq_csv_file_fields, entry)
      #if entry_w_fields_dict["sequence"] in self.seq_ids_by_name_dict:
      sequence_id       = self.seq_ids_by_name_dict[entry_w_fields_dict["sequence"]]
      silva_taxonomy_id = taxonomy.silva_taxonomy_id_per_taxonomy_dict[entry_w_fields_dict["taxonomy"]]
      gast_distance     = entry_w_fields_dict["distance"]
      # refssu_id         =
      # refssu_count      =
      rank_id           = self.utils.find_val_in_nested_list(taxonomy.all_rank_w_id, entry_w_fields_dict["rank"])[0]
      if silva_taxonomy_id != [] and silva_taxonomy_id != '[]':
        temp_list = list((sequence_id, silva_taxonomy_id, gast_distance, refssu_id, refssu_count, rank_id))
        self.silva_taxonomy_info_per_seq_list.append(temp_list)

    # self.utils.print_array_w_title(self.silva_taxonomy_info_per_seq_list, "self.silva_taxonomy_info_per_seq_list from silva_taxonomy_info_per_seq_from_csv = ")

  def insert_silva_taxonomy_info_per_seq(self):
    # self.silva_taxonomy_info_per_seq_list = [[8559950L, 2436599, '0.03900', 0, 0, 83],...
    field_list = "sequence_id, silva_taxonomy_id, gast_distance, refssu_id, refssu_count, rank_id"
    
    # sql = "INSERT %s INTO `%s` (`%s`) VALUES (%s)" % ("IGNORE", "silva_taxonomy_info_per_seq", field_list, all_insert_dat_vals)
    # self.utils.print_array_w_title(sql, "sql")
    #print('self.silva_taxonomy_info_per_seq_list')
    #print(self.silva_taxonomy_info_per_seq_list)
    if len(self.silva_taxonomy_info_per_seq_list) > self.utils.min_seqs and '[]' not in self.silva_taxonomy_info_per_seq_list:
        split = len(self.silva_taxonomy_info_per_seq_list)/self.utils.chunk_split  # how many pieces
        for i,vals in enumerate(self.utils.chunks(self.silva_taxonomy_info_per_seq_list, int(math.ceil(split)))):
            all_insert_dat_vals = self.utils.make_insert_values(vals)
            print(i+1,'/',self.utils.chunk_split,' -- len vals chunk:',len(all_insert_dat_vals))
            rows_affected = mysql_util.execute_insert("silva_taxonomy_info_per_seq", field_list, all_insert_dat_vals)
            self.utils.print_array_w_title(rows_affected, "rows_affected by insert_silva_taxonomy_info_per_seq")
    else:
        all_insert_dat_vals = self.utils.make_insert_values(self.silva_taxonomy_info_per_seq_list)
        rows_affected = mysql_util.execute_insert("silva_taxonomy_info_per_seq", field_list, all_insert_dat_vals)
        self.utils.print_array_w_title(rows_affected, "rows_affected by insert_silva_taxonomy_info_per_seq")

  def parse_env_sample_source_id(self):
    # mysql -B -h vampsdb vamps -e "select env_sample_source_id, env_source_name from new_env_sample_source" >env_sample_source_id.csv
    pass

  def check_env_sample_source_id(self):
    # TODO: check env_source_id/env_sample_source_id in project, if not in env_sample_source_id.csv - change to 0
    pass

  def get_seq_id_w_silva_taxonomy_info_per_seq_id(self):
    sequence_ids_strs = [str(id) for id in self.seq_ids_by_name_dict.values()]
    where_part = 'WHERE sequence_id in (%s)' % ', '.join(sequence_ids_strs)
    self.seq_id_w_silva_taxonomy_info_per_seq_id = mysql_util.get_all_name_id("silva_taxonomy_info_per_seq", "silva_taxonomy_info_per_seq_id", "sequence_id", where_part)

  def sequence_uniq_info_from_csv(self, sequences_w_ids):
    self.get_seq_id_w_silva_taxonomy_info_per_seq_id()
    # ! sequence_uniq_info (sequence_id, silva_taxonomy_info_per_seq_id, gg_otu_id, oligotype_id)
    # print("sequence_uniq_info_values = %s" % sequence_uniq_info_values)

  def insert_sequence_uniq_info(self):
    field_list = "sequence_id, silva_taxonomy_info_per_seq_id"
    if len(self.seq_id_w_silva_taxonomy_info_per_seq_id) > self.utils.min_seqs:
        split = len(self.seq_id_w_silva_taxonomy_info_per_seq_id)/self.utils.chunk_split  # how many pieces
        for i,vals in enumerate(self.utils.chunks(self.seq_id_w_silva_taxonomy_info_per_seq_id, int(math.ceil(split)))):
            sequence_uniq_info_values = '), ('.join(str(i1) + "," + str(i2) for i1, i2 in vals)
            print(i+1,'/',self.utils.chunk_split,' -- len vals chunk:',len(sequence_uniq_info_values))
            rows_affected = mysql_util.execute_insert("sequence_uniq_info", field_list, sequence_uniq_info_values)
            self.utils.print_array_w_title(rows_affected, "rows_affected from insert_sequence_uniq_info = ")
    else:
        self.sequence_uniq_info_values = '), ('.join(str(i1) + "," + str(i2) for i1, i2 in self.seq_id_w_silva_taxonomy_info_per_seq_id)
        
        rows_affected = mysql_util.execute_insert("sequence_uniq_info", field_list, self.sequence_uniq_info_values)
        
        self.utils.print_array_w_title(rows_affected, "rows_affected from insert_sequence_uniq_info = ")



    """
    ***) simple tables:
          domain
          family
          genus
          klass
          order
          phylum
          refhvr_id
          sequence
          species
          strain
          user

    ***) tables with foreign keys (SELECT distinct referenced_table_name FROM information_schema.KEY_COLUMN_USAGE WHERE table_schema = "vamps2" and referenced_table_schema = "vamps2";):
          project
          dataset
          sequence_pdr_info
        silva_taxonomy
        sequence_uniq_info

    ***) ref tables
        ref_silva_taxonomy_info_per_seq_refhvr_id
        silva_taxonomy_info_per_seq

    """

class Metadata:
  """
  custom_metadata fields are per project,
  but data could be by dataset

 
  required_metadata_info (dataset_id, taxon_id, description, common_name, altitude, assigned_from_geo, collection_date, depth, geo_loc_name, elevation, env_biome, env_feature, env_material, latitude, longitude, public)
  custom_metadata_fields (project_id, field_name, field_units, example)
  
  create all metadata values
  create custom table
  separately insert req and custom
  
  ---
  parse_metadata_csv(self, metadata_csv_file_name)
  
  get_parameter_by_dataset_dict(self)
  
  get_existing_field_names(self)
  
  get_existing_required_metadata_fields(self)
  
  custom_metadata_fields_tbls:
  get_existing_custom_metadata_fields(self)
  data_for_custom_metadata_fields_table(self, project_dict)
  insert_custom_metadata_fields(self)
  get_data_from_custom_metadata_fields(self)
  make_data_from_custom_metadata_fields_dict(self, custom_metadata_field_data_res)
  create_custom_metadata_pr_id_table(self)
  
  make_param_per_dataset_dict(self)    
  
  """

  def __init__(self, mysql_util, dataset, project_dict):
    self.utils = Utils()
    self.project_dict                    = project_dict
    self.metadata_file_fields            = []
    self.metadata_file_content           = []
    self.metadata_w_names                = []
    self.required_metadata_info_fields   = ["dataset_id",                                        
                                            "collection_date",
                                            "latitude", 
                                            "longitude",                                            
                                            "target_gene_id",   # 16s or 18s FROM NAME
                                            "dna_region_id",     # v6 v3 v4v6 .... FROM NAME
                                            "sequencing_platform_id", # 454 or illumina ???
                                            "domain_id",            # Bacteria, Archaea....FROM NAME
                                            "geo_loc_name_id",           #  from term table ???
                                            "env_feature_id",      
                                            "env_material_id",       
                                            "env_biome_id",        
                                            "env_package_id",
                                            "adapter_sequence",
                                            "illumina_index",
                                            "primer_suite",
                                            "run"
                                            ]
    
    self.substitute_field_names          = { 
                                            "latitude" : ["lat","LATITUDE"], 
                                            "longitude": ["long", "lon","LONGITUDE"],
                                            #"env_biome_id": ["envo_biome","ENV_BIOME","ENVO_BIOME"], \
                                            #"env_material_id":["envo_material","envo_material","env_meterial","ENV_MATTER","ENVO_MATTER"], \
                                            #"env_feature_id":["envo_feature","ENV_FEATURE","ENVO_FEATURE"], \                                           
                                            "dataset_id":["DATASET_ID"],
                                            "collection_date":["COLLECTION_DATE"], 
                                            "run":["rundate"]
                                            }
    self.existing_field_names            = set()
    self.required_metadata               = []
    self.required_metadata_insert_values = ""
    self.required_metadata_field_list    = ""
    # multilevel_dict                      = lambda: defaultdict(multilevel_dict)
    
    self.existing_required_metadata_fields        = {}
    self.custom_metadata_fields_insert_values     = ""
    self.custom_metadata_fields_uniqued_for_tbl   = []
    self.project_ids                              = set()
    self.custom_metadata_per_project_dataset_dict = defaultdict(lambda: defaultdict(dict))
    
    self.get_target_gene_ids()             # args.target_gene
    self.get_dna_region_ids()           # args.dna_region
    self.get_domain_ids()               # args.domain
    self.get_sequencing_platform_ids()  # args.platform
    #self.get_geo_loc_name_ids()              # args.geo_loc_name
    self.get_term_ids()                 # args.env_material, args.env_feature, args.env_biome
    self.get_package_ids()              # args.env_package
    self.get_adapter_sequence_ids()
    self.get_illumina_index_ids()
    self.get_primer_suite_ids()
    self.get_run_ids()
    
    #print('self.target_gene_list',self.target_gene_list)
    print('self.dna_region_list',self.dna_region_list)
    #print('self.domain_list',self.domain_list)
    print('self.sequencing_platform_list',self.sequencing_platform_list)
    #print('self.geo_loc_name_list',self.geo_loc_name_list)
    print('self.package_list',self.package_list)
    #print('self.adapter_sequence_list',self.adapter_sequence_list)
    print('self.illumina_index_list',self.illumina_index_list)
    print('self.primer_suite_list',self.primer_suite_list)
    #print('self.run_list',self.run_list)
    
    (target_gene, self.target_gene_id)              = self.find_required_id('target_gene', args.target_gene)
    (dna_region,self.dna_region_id)                  = self.find_required_id('dna_region', args.dna_region)
    (domain,self.domain_id)                          = self.find_required_id('domain', args.domain)
    (sequencing_platform,self.sequencing_platform_id)= self.find_required_id('platform', args.platform)
    (geo_loc_name,self.geo_loc_name_id)              = self.find_required_id('geo_loc_name', args.geo_loc_name)
    (env_material,self.env_material_id)                  = self.find_required_id('env_material', args.env_material)
    (env_feature,self.env_feature_id)                = self.find_required_id('env_feature', args.env_feature)
    (env_biome,self.env_biome_id)                    = self.find_required_id('env_biome', args.env_biome)
    (env_package,self.env_package_id)                = self.find_required_id('env_package', args.env_package)
    (adapter_sequence,self.adapter_sequence_id)      = self.find_required_id('adapter_sequence', args.adapter_sequence)
    (illumina_index,self.illumina_index_id)          = self.find_required_id('illumina_index', args.illumina_index)
    (primer_suite,self.primer_suite_id)              = self.find_required_id('primer_suite', args.primer_suite)
    (run,self.run_id)                               = self.find_required_id('run', args.run)
    
    self.report = ''
    self.report += '\nProject::\t'+args.project+'\n'
    self.report += 'target_gene::\t'+target_gene+' (id:'+self.target_gene_id+')'+'\n'
    self.report += 'dna_region::\t'+dna_region+' (id:'+self.dna_region_id+')'+'\n'
    self.report += 'domain::\t'+domain+' (id:'+self.domain_id+')'+'\n'
    self.report += 'platform::\t'+sequencing_platform+' (id:'+self.sequencing_platform_id+')'+'\n'
    self.report += 'geo_loc_name::\t'+geo_loc_name+' (id:'+self.geo_loc_name_id+')'+'\n'
    self.report += 'env_material::\t'+env_material+' (id:'+self.env_material_id+')'+'\n'
    self.report += 'env_feature::\t'+env_feature+' (id:'+self.env_feature_id+')'+'\n'
    self.report += 'env_biome::\t'+env_biome+' (id:'+self.env_biome_id+')'+'\n'
    self.report += 'env_package::\t'+env_package+' (id:'+self.env_package_id+')'+'\n'
    self.report += 'adapter_seq::\t'+adapter_sequence+' (id:'+self.adapter_sequence_id+')'+'\n'
    self.report += 'illumina_idx::\t'+illumina_index+' (id:'+self.illumina_index_id+')'+'\n'
    self.report += 'primer_suite::\t'+primer_suite+' (id:'+self.primer_suite_id+')'+'\n'
    self.report += 'run::\t\t'+run+' (id:'+self.run_id+')'+'\n'
    self.report += 'public::\t\t'+args.public+'\n'

  
  def find_required_id(self, term, req_name):
    id = 'id_not_found'
    name = 'unknown'
    
   
    #    req_name = 'unknown'
    if term == 'target_gene':
        list = self.target_gene_list
    elif term == 'dna_region':
        list = self.dna_region_list
    elif term == 'domain':
        list = self.domain_list
    elif term == 'platform':
        list = self.sequencing_platform_list
    elif term == 'geo_loc_name':
        list = self.term_list
    elif term == 'env_material':
        list = self.term_list
    elif term == 'env_feature':
        list = self.term_list
    elif term == 'env_biome':
        list = self.term_list
    elif term == 'env_package':
        list = self.package_list        
    elif term == 'adapter_sequence':
        list = self.adapter_sequence_list
    elif term == 'illumina_index':
        list = self.illumina_index_list
    elif term == 'primer_suite':
        list = self.primer_suite_list
    elif term == 'run':
        list = self.run_list
    else:
        id='no list!!'    
    
    for t in list:
        if not req_name and (t[1].lower() == 'unknown' or t[1].lower() == 'undefined'):
            id =str(t[0])
            name = t[1]
            break
        elif t[1].lower() == req_name.lower():
            id = str(t[0])
            name = t[1]
            break
        else:
            id =str(t[0])
            name = 'unknown'
    return (name,id)
  
  def parse_metadata_csv(self, metadata_csv_file_name):
    print("=" * 20)
    print(metadata_csv_file_name)
    self.metadata_file_fields, self.metadata_file_content = self.utils.read_csv_into_list(metadata_csv_file_name)
    
    """
    metadata_ICM_SMS_Bv6_short.csv
    ['dataset', 'parameterName', 'parameterValue', 'units', 'miens_units', 'project', 'units_id', 'structured_comment_name', 'method', 'other', 'notes', 'ts', 'entry_date', 'parameter_id', 'project_dataset']
    [['SMS_0001_2007_09_19', 'domain', 'Bacteria', 'Alphanumeric', 'Alphanumeric', 'ICM_SMS_Bv6', '1', 'domain', '', '0', 'sms.txt  2009-03-31 PRN  miens update prn 2010_05_19 miens update units --prn 2010_05_19', '2012-04-27 08:25:07', '', '0', 'ICM_SMS_Bv6--SMS_0001_2007_09_19']
    """
    # self.get_parameter_by_project_dict()
    # self.get_parameter_by_dataset_dict()
  
  def add_names_to_params(self):
    self.metadata_w_names = [utils.make_entry_w_fields_dict(self.metadata_file_fields, row) for row in self.metadata_file_content]
   
    '''
[{'parameter_id': '0',
'notes': 'acb.txt  2009-06-22 PRN  miens update prn 2010_05_19 miens update units --prn 2010_05_19',
'structured_comment_name': 'cruise',
'ts': '2012-04-27 08:25:07',
'dataset': 'ACB_0009_2007_07_13',
'project': 'ICM_ACB_Av6',
'miens_units': 'Alphanumeric',
'parameterValue': '707',
'other': '0',
'entry_date': '',
'project_dataset': 'ICM_ACB_Av6--ACB_0009_2007_07_13',
'units': 'Alphanumeric',
'parameterName': 'cruise',
'method': '',
'units_id': '1'},
{'parameter_id': '0', ...}]
      
      '''
    
  def add_ids_to_params(self):
   
    for param_per_dataset in self.metadata_w_names:
      print("PPP param_per_dataset:")
      print(param_per_dataset)
      if param_per_dataset['dataset'] in dataset.dataset_id_by_name_dict:
        param_per_dataset['dataset_id'] = dataset.dataset_id_by_name_dict[param_per_dataset['dataset']]
       
      param_per_dataset['project_id'] = self.project_dict[param_per_dataset['project']]
          

  # ==== Fields =====
  def get_existing_field_names(self):
    self.existing_field_names = set([(self.correct_field_name(param_per_dataset['structured_comment_name'])) for param_per_dataset in self.metadata_w_names])
    #self.existing_field_names_for_required_match = set([(self.correct_field_name(param_per_dataset['structured_comment_name'])).lower() for param_per_dataset in self.metadata_w_names])
  def get_existing_required_metadata_fields(self):
    
    intersect_field_names = self.existing_field_names.intersection(self.required_metadata_info_fields) 
    
    for field_name in intersect_field_names:
      self.existing_required_metadata_fields[field_name] = field_name

    for good_name, bad_name_list in self.substitute_field_names.items():
      bad_and_exist_intersection = self.existing_field_names.intersection(bad_name_list) 
      for existing_field_name in bad_and_exist_intersection:
        self.existing_required_metadata_fields[good_name] = existing_field_name
    
  def get_existing_custom_metadata_fields(self):
    self.custom_metadata_fields = self.existing_field_names ^ set(self.existing_required_metadata_fields.values())    

  # ==== Required metadata =====
  def prepare_required_metadata(self):
    
    structured_comment_names = set([param_per_dataset['structured_comment_name'] for param_per_dataset in self.metadata_w_names])
    existing_required_metadata_fields_values_per_dataset = defaultdict(dict)
    
    for param_per_dataset in self.metadata_w_names:
      existing_required_metadata_fields_values_per_dataset[param_per_dataset['dataset']][param_per_dataset['structured_comment_name']] = param_per_dataset['parameterValue']
    
    intr = structured_comment_names.intersection(self.existing_required_metadata_fields.values())
   
    for dataset_name, metadata in existing_required_metadata_fields_values_per_dataset.items():    
      if dataset_name in dataset.dataset_id_by_name_dict:
          dataset_id = dataset.dataset_id_by_name_dict[dataset_name]
          temp_dict = {}
          for field_name in list(intr):
            key = self.utils.find_key_by_value_in_dict(self.existing_required_metadata_fields.items(), str(field_name))
            """
              AAV::  If field_name is absent this will add a blank rather than crashing
            """
           
            if field_name in metadata:
                temp_dict[key[0]] = metadata[field_name]
            else:
                temp_dict[key[0]] = ''
          temp_dict['dataset_id'] = str(dataset_id)
          self.required_metadata.append(temp_dict)
   
      
  def required_metadata_for_insert(self):
    all_required_metadata = []
    field_list_temp       = []
    
    for required_metadata_dict in self.required_metadata:      
      field_list_temp.append(required_metadata_dict.keys())
      all_required_metadata.append(required_metadata_dict.values())
    add_on_names = [self.target_gene_id, self.dna_region_id, self.domain_id, self.sequencing_platform_id, self.geo_loc_name_id, self.env_biome_id,self.env_feature_id,self.env_material_id,self.env_package_id,self.adapter_sequence_id,self.illumina_index_id,self.primer_suite_id,self.run_id]
    #print('add_on_names')
    #print(add_on_names)
    #print('all_required_metadata')
    #print(all_required_metadata)
    if len(all_required_metadata) > 0:
        
        all_required_metadata = [list(x) + add_on_names for x in all_required_metadata]
        
        self.required_metadata_insert_values = self.utils.make_insert_values(all_required_metadata)      
        self.required_metadata_field_list    = ", ".join(field_list_temp[0])
        self.required_metadata_field_list += ", "
        
    else:
        # NO file metadata
        self.required_metadata_insert_values = []      
        self.required_metadata_field_list = 'dataset_id,'   # needed first item
        for dset in dataset.dataset_id_by_name_dict:
            self.required_metadata_insert_values.append([dataset.dataset_id_by_name_dict[dset]] + add_on_names)  #prepend did
        self.required_metadata_insert_values = self.utils.make_insert_values(self.required_metadata_insert_values) 
    #all_required_metadata = [x + add_on_names for x in all_required_metadata]
    self.required_metadata_field_list += "target_gene_id, dna_region_id, domain_id, sequencing_platform_id, geo_loc_name_id, env_biome_id, env_feature_id, env_material_id, env_package_id,adapter_sequence_id,illumina_index_id,primer_suite_id,run_id"
    return
    
    
  def insert_required_metadata(self):
    if self.required_metadata_insert_values:
        rows_affected = mysql_util.execute_insert("required_metadata_info", self.required_metadata_field_list, self.required_metadata_insert_values)    
        self.utils.print_array_w_title(rows_affected, "rows_affected from insert_required_metadata")
    else:
        print('No required metadata found or entered.')
  # ==== Custom metadata =====
  
  # add fields per dataset to custom_metadata_fields (project_id, field_name, field_units, example)
  # create table per project
  # add data to the table per project

  # add fields per dataset to custom_metadata_fields (project_id, field_name, field_units, example)
  def data_for_custom_metadata_fields_table(self):
    # TODO: too long and does more then one thing - change!
    custom_metadata_fields_for_tbl         = []
    custom_metadata_fields_uniqued_for_tbl = []
    print('self.custom_metadata_fields')
   
    for param_per_dataset in self.metadata_w_names:
      field_name = self.correct_field_name(param_per_dataset['structured_comment_name'])
      if field_name in self.custom_metadata_fields:      
        project_id  = param_per_dataset['project_id']
        field_units = param_per_dataset['miens_units']
        example     = param_per_dataset['parameterValue'].replace("'","")
        custom_metadata_fields_for_tbl.append((project_id, field_name, field_units, example))
        custom_metadata_fields_uniqued_for_tbl.append((project_id, field_name, field_units))
      
    # just slightly faster: custom_metadata_fields_for_tbl = [(param_per_dataset['project_id'], param_per_dataset['structured_comment_name'], param_per_dataset['miens_units'], param_per_dataset['parameterValue']) for param_per_dataset in self.metadata_w_names]
    if len(custom_metadata_fields_for_tbl) >0:
        self.custom_metadata_fields_uniqued_for_tbl = list(set(custom_metadata_fields_uniqued_for_tbl))
        self.custom_metadata_fields_insert_values   = self.utils.make_insert_values(list(set(custom_metadata_fields_for_tbl)))
    
    else:
        self.custom_metadata_fields_uniqued_for_tbl = []
        self.custom_metadata_fields_insert_values   = []
        #self.project_ids = []
    self.project_ids                            = set([e[0] for e in self.custom_metadata_fields_uniqued_for_tbl])
        
  def insert_custom_metadata_fields(self):
    field_list = "project_id, field_name, field_units, example"
    '''
    ('275', 'aux_corrected_depth', 'unknown', '3952'),
    ('275', 'aux_corrected_depth', 'unknown', '3959'),
    means that "example" is one of them
    '''

    # aux_bec_simulated_phosphate__um_
   
    if self.custom_metadata_fields_insert_values:
        rows_affected = mysql_util.execute_insert("custom_metadata_fields", field_list, self.custom_metadata_fields_insert_values)
        self.utils.print_array_w_title(rows_affected, "rows_affected from insert_custom_metadata_fields")
    else:
        print('No custom metadata found or entered.')

  # create table per project
  def get_data_from_custom_metadata_fields(self, project_dict):
    field_names = "project_id, field_name, field_units"
    table_name  = "custom_metadata_fields"
    where_part  = "WHERE project_id in (%s)" % (",".join(map(str, project_dict.values())))
    self.custom_metadata_fields_uniqued_for_tbl = mysql_util.execute_simple_select(field_names, table_name, where_part)

  def correct_field_name(self, field_name):
    return field_name.replace("(", "_").replace(")", "_").replace(",", "-").lower()  # LOWERCASE decided for all fields 2017-11-29
    
  def create_custom_metadata_pr_id_table(self):    
    
    # set([275])
    
    # [(275, 'aux_bec_simulated_nitrate_(um)', 'unknown'), (275, 'depth_end', 'meter'), (275, 'aux_bec_simulated_phosphate_(um)', 'unknown'), (275, 'aux_sunset_hr', 'unknown'), 
    for project_id in self.project_ids:
        field_descriptions  = ""
        table_name          = "custom_metadata_%s" % project_id
        id_name             = "%s_id" % (table_name)
        primary_key_field   = "%s int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" % (id_name)
    
        field_descriptions  = primary_key_field + "`dataset_id` int(11) unsigned NOT NULL,\n"
        """AAV:: For common long metadata entries the varchar(128) should get replaced with 'text'        """
        wordy_fields = ['experiment_design_description','library_construction_protocol','study_abstract','STUDY_DESCRIPTION','study_description'] # mainly MBE
        
        fields = []
        for entry in self.custom_metadata_fields_uniqued_for_tbl:
            if entry[1].lower() in wordy_fields:
                #field_descriptions += "`%s` text DEFAULT NULL,\n" % (entry[1])
                fields.append("`%s` text DEFAULT NULL" % (entry[1]))
            else:
                #field_descriptions += "`%s` varchar(128) DEFAULT NULL,\n" % (entry[1])
                fields.append("`%s` varchar(128) DEFAULT NULL" % (entry[1]))
        fields = list(set(fields))
        field_descriptions += ',\n'.join(fields)+","
        field_descriptions += """
            UNIQUE KEY dataset_id (dataset_id),
            CONSTRAINT %s_ibfk_1 FOREIGN KEY (dataset_id) REFERENCES dataset (dataset_id) ON UPDATE CASCADE
            """ % (table_name)
        
        """AAV - Want to start with a clean table"""
        q = "DROP TABLE IF EXISTS %s" % (table_name)
        print(mysql_util.execute_no_fetch(q))
        
        table_description = "ENGINE=InnoDB"
        print('field_descriptions')
        print(field_descriptions)
        print('field_descriptions')
        #sys.exit()
        #field_descriptions = list(set(field_descriptions)) ## THIS uniques
        q = "CREATE table %s (%s) %s" % (table_name, field_descriptions, table_description)
        # should unique the field_descriptions
        
        print(mysql_util.execute_no_fetch(q))

  def make_custom_metadata_per_project_dataset_dict(self):
    for param_per_dataset in self.metadata_w_names:
      field_name  = self.correct_field_name(param_per_dataset['structured_comment_name'])
      if field_name in self.custom_metadata_fields:              
        project_id  = str(param_per_dataset['project_id'])
        if 'dataset_id' in param_per_dataset:
            dataset_id  = str(param_per_dataset['dataset_id'])
            param_value = str(param_per_dataset['parameterValue'])
            self.custom_metadata_per_project_dataset_dict[project_id][dataset_id][field_name] = param_value

  def insert_custom_metadata(self):
    # TODO: simplify
    self.make_custom_metadata_per_project_dataset_dict()
    field_str = "dataset_id, "  + "`"   + "`, `".join(self.custom_metadata_fields)   + "`"
    for project_id, custom_metadata_dict_per_dataset in self.custom_metadata_per_project_dataset_dict.items():
      custom_metadata_table_name = "custom_metadata_%s" % project_id
      insert_values              = ""
      insert_values_list_2d      = []
      for dataset_id, custom_metadata_dict in custom_metadata_dict_per_dataset.items():
        insert_values_temp = [str(dataset_id)]
        for field_name in self.custom_metadata_fields:
          try:
            insert_values_temp.append(custom_metadata_dict[field_name].replace("'",""))
          except KeyError: 
            insert_values_temp.append('')
          except:
            raise
        insert_values_list_2d.append(insert_values_temp)
            
      insert_values = self.utils.make_insert_values(insert_values_list_2d)
      
      rows_affected = mysql_util.execute_insert(custom_metadata_table_name, field_str, insert_values)
      self.utils.print_array_w_title(rows_affected, "rows affected by insert_custom_metadata")
      
  def get_target_gene_ids(self):
    field_names = "target_gene_id, target_gene"
    table_name  = "target_gene"
    where_part  = ""
    self.target_gene_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
   

  def get_dna_region_ids(self):
    field_names = "dna_region_id, dna_region"
    table_name  = "dna_region"
    where_part  = ""
    self.dna_region_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_domain_ids(self):
    field_names = "domain_id, domain"
    table_name  = "domain"
    where_part  = ""
    self.domain_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_sequencing_platform_ids(self):
    field_names = "sequencing_platform_id, sequencing_platform"
    table_name  = "sequencing_platform"
    where_part  = ""
    self.sequencing_platform_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
   
    
  def get_term_ids(self):
    field_names = "term_id, term_name"
    table_name  = "term"
    where_part  = ""
    self.term_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_package_ids(self):
    field_names = "env_package_id, env_package"
    table_name  = "env_package"
    where_part  = ""
    self.package_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_adapter_sequence_ids(self):
    field_names = "run_key_id, run_key"
    table_name  = "run_key"
    where_part  = ""
    self.adapter_sequence_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_illumina_index_ids(self):
    field_names = "illumina_index_id, illumina_index"
    table_name  = "illumina_index"
    where_part  = ""
    self.illumina_index_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_primer_suite_ids(self):
    field_names = "primer_suite_id, primer_suite"
    table_name  = "primer_suite"
    where_part  = ""
    self.primer_suite_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
  def get_run_ids(self):
    field_names = "run_id, run"
    table_name  = "run"
    where_part  = ""
    self.run_list = mysql_util.execute_simple_select(field_names, table_name, where_part)
    
    
if __name__ == '__main__':
  import subprocess
  import argparse
  
  parser = argparse.ArgumentParser(description = "")
  
  myusage = """
  
        old_2_new_vamps_by_project.py
        
        -p/--project                    REQUIRED
        -public/--public        0 or 1 default: 1 (public)
        -d/--delimiter          Metadata file default: ','
        -w/--write_files        currently not used: use ../export_project.sh before
                                running this script
        -ni/--do_not_insert     for testing
        -nu/--do_not_update     ***Do not update sequence taxonomy if different
        -mo/--metadata_only
        -host/--host                    REQUIRED vamps or vampsdev or local
        -non_mbl_format         If project is not in XX_YYY_ZZ format

    For these the entered name MUST match name in New VAMPS table
        -platform/--sequencing_platform   default: unknown
        -geo_loc_name/--geo_loc_name      default: unknown
        -target_gene/--target_gene      default: unknown
        -domain/--domain
        -env_biome/--env_biome            default: unknown
        -env_material/--env_material
        -env_feature/--env_feature
        -env_package/--env_package
        -illumina_index/--illumina_index
        -run_key/--run_key
        -primer_suite/--primer_suite
        -run/--run



  """
  parser.add_argument("-p","--project",
      required = True, action = "store", dest = "project", default = '',
      help = """ProjectID""")
  parser.add_argument("-public","--public",
      required = False, action = "store", choices=['0', '1'], dest = "public", default = '1',
      help = """0 (private) or 1 (public)""")
  parser.add_argument("-d","--delimiter",
      required = False, action = "store", dest = "delim", default = ',',
      help = """METADATA: comma or tab""")
  parser.add_argument("-w","--write_files",
      required = False, action = "store_true", dest = "write_files",
      help = """Create csv files first""")
  parser.add_argument("-ni","--do_not_insert",
      required = False, action = "store_true", dest = "do_not_insert", default = False,
      help = """Do not insert data into db, mostly for debugging purposes""")
  parser.add_argument("-nu","--do_not_update",
      required = False, action = "store_true", dest = "do_not_update", default = False,
      help = """Do not update taxonomy--sequence link, mostly for testing purposes""")
  parser.add_argument("-mo","--metadata_only",
      required = False, action = "store_true", dest = "metadata_only", default = False,
      help = """No seqs or taxonomy - just metadata""")    
  parser.add_argument("-host", "--host",
        required = True, action = "store", dest = "host", 
        help = """Site where the script is running""")
  
  # Required Metadata
  parser.add_argument("-platform","--sequencing_platform",
      required = False, action = "store", dest = "platform", default = '',
      help = """Sequencing platform -- must match text in newVAMPS Table""")
  parser.add_argument("-geo_loc_name","--geo_loc_name",
      required = False, action = "store", dest = "geo_loc_name", default = '',
      help = """geo_loc_name -- must match text in newVAMPS Table""")    
  parser.add_argument("-target_gene", "--target_gene",
        required = False, action = "store", dest = "target_gene", default='',
        help = """target_gene Name -- no entry results in 'unknown' """)
  parser.add_argument("-domain", "--domain",
        required = False, action = "store", dest = "domain", default='',
        help = """Domain -- no entry results in 'unknown' """)   
  parser.add_argument("-dna_region", "--dna_region",
        required = False, action = "store", dest = "dna_region", default='',
        help = """dna_region -- no entry results in 'unknown' """) 
  parser.add_argument("-env_biome", "--env_biome",
        required = False, action = "store", dest = "env_biome", default = '',
        help = """ENV_Biome -- must match text in newVAMPS Table""")
  parser.add_argument("-env_feature", "--env_feature",
        required = False, action = "store", dest = "env_feature", default='',
        help = """env_feature -- no entry results in 'unknown' """)
  parser.add_argument("-env_material", "--env_material",
        required = False, action = "store", dest = "env_material", default='',
        help = """env_material -- no entry results in 'unknown' """)
  parser.add_argument("-env_package", "--env_package",
        required = False, action = "store", dest = "env_package", default='',
        help = """env_package -- no entry results in 'unknown' """) 
  parser.add_argument("-run_key", "--run_key",
        required = False, action = "store", dest = "adapter_sequence", default='',
        help = """run_key (adapter_sequence) -- no entry results in 'unknown' """)
  parser.add_argument("-illumina_index", "--illumina_index",
        required = False, action = "store", dest = "illumina_index", default='',
        help = """illumina_index -- no entry results in 'unknown' """)
  parser.add_argument("-primer_suite", "--primer_suite",
        required = False, action = "store", dest = "primer_suite", default='',
        help = """primer_suite -- no entry results in 'unknown' """) 
  parser.add_argument("-run", "--run",
        required = False, action = "store", dest = "run", default='',
        help = """run -- no entry results in 'unknown' """) 
  parser.add_argument("-non_mbl_format", "--non_mbl_format",
        required = False, action = "store_true", dest = "non_mbl_format", default=False,
        help = """MBL Format: three parts; end with Bv6, Ev4... """)      
            
    
  
  if len(sys.argv[1:]) == 0:
        print(myusage)
        sys.exit() 
  args = parser.parse_args()
  print(args)
  if args.non_mbl_format:
    print('This is a non MBL Formatted project -- using command line metadata')
  else:
    pparts = args.project.split('_') 
    print(pparts   )
    if len(pparts) != 3:
        sys.exit('project in not formatted correctly. Use (-non_mbl_format) and command line metadata for domain, dna_region, primer_suite, target_gene')
    
    if pparts[2].startswith('A'):
        args.domain = 'Archaea'
        args.target_gene = '16s'
        
    elif pparts[2].startswith('B'):
        args.domain = 'Bacteria'
        args.target_gene = '16s'
    elif pparts[2].startswith('E'):
        args.domain = 'Eukarya'
        args.target_gene = '18s'
    else:
        print('project suffix is not in A(rchaea)-B(acteria)-E(ukarya)-') 
        #args.domain = 'unknown'
        #args.target_gene = 'unknown'
        
    args.dna_region = pparts[2][1:]
    args.primer_suite =  args.domain
    if args.dna_region == 'v6':
        args.primer_suite +='l V6 Suite'
    elif args.dna_region == 'v4':
        args.primer_suite +='l V4 Suite'
    elif args.dna_region == 'v4v5':
        args.primer_suite +='l V4-V5 Suite'
    elif args.dna_region == 'v6v4':
        args.primer_suite +='l V6-V4 Suite'
    elif args.dna_region == 'v3':
        args.primer_suite +='l V3 Suite'
    elif args.dna_region == 'v3v1':
        args.primer_suite +='l V3-V1 Suite'
    elif args.dna_region == 'v3v5':
        args.primer_suite +='l V3-V5 Suite'
    elif args.dna_region == 'v4v6':
        args.primer_suite +='l V4-V6 Suite'
    elif args.dna_region == 'v5v3':
        args.primer_suite +='l V5-V3 Suite'
    elif args.dna_region == 'v9':
        args.primer_suite +='l V9 Suite'
    else:
        print('No good primer suite found')
  
  utils = Utils()
  
  print("args = ")
  print(args)
  print("args.write_files")
  print(args.write_files)

  host_prod   = "bpcweb7"
  to_database = 'vamps2'
  if args.host == 'vamps' or args.host == 'vampsdb':
      host_prod = "vampsdb"
  elif args.host=='vampsdev':
      host_prod = "bpcweb7"
  else:  ## localhost
      host_prod = "localhost"
      to_database = "vamps_development"
  if (args.write_files == True):
     csv_files = CSV_files()
# 
  
  
   
#     t0 = time.time()
#     if args.metadata_only:
#         metadata_csv_file_name = csv_files.run_md_dump(prod_mysql_util)
#     else:
#         metadata_csv_file_name, seq_csv_file_name, project_csv_file_name, dataset_csv_file_name, user_contact_csv_file_name = csv_files.run_csv_dump(prod_mysql_util)
#     t1 = time.time()
#     total = t1-t0

    
 # else:
# todo: get file_names and path from args
  """
    USE export_project.sh script
    OR Create manually:  
    mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_metadata where project = 'ICM_SMS_Bv6';" | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > metadata_ICM_SMS_Bv6.csv

    mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences where project = 'ICM_SMS_Bv6';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > sequences_ICM_SMS_Bv6.csv

    mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences_pipe where project = 'ICM_SMS_Bv6';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" >> sequences_ICM_SMS_Bv6.csv

    mysql -B -h vampsdb vamps -e "SELECT project, title, project_description, funding, env_sample_source_id, contact, email, institution FROM new_project LEFT JOIN new_contact using(contact_id) WHERE project = 'ICM_SMS_Bv6' UNION SELECT project_name AS project, title, description AS project_description, 0 AS funding, env_source_id AS env_sample_source_id, contact, email, institution FROM vamps_upload_info WHERE project_name = 'ICM_SMS_Bv6';" | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > project_ICM_SMS_Bv6.csv

    mysql -B -h vampsdb vamps -e "SELECT distinct dataset, dataset_description, env_sample_source_id, project from new_dataset join new_project using(project_id) WHERE project = 'ICM_SMS_Bv6';" | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > dataset_ICM_SMS_Bv6.csv

    mysql -B -h vampsdb vamps -e "SELECT distinct contact, user as username, email, institution, first_name, last_name, active, security_level, passwd as encrypted_password from new_user_contact join new_user using(user_id) join new_contact using(contact_id) where first_name is not NULL and first_name <> '';" | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" >> user_contact_ICM_SMS_Bv6.csv

  """

  # TODO: add names from args here
  
  
  user_contact_csv_file_name = "user_contact_%s.csv" % (args.project)
  project_csv_file_name      = "project_%s.csv" % (args.project)
  dataset_csv_file_name      = "dataset_%s.csv" % (args.project)
  seq_csv_file_name          = "sequences_%s.csv" % (args.project)
  metadata_csv_file_name     = "metadata_%s.csv" % (args.project)

  # ========

  print("metadata_csv_file_name = %s, seq_csv_file_name = %s, project_csv_file_name = %s, dataset_csv_file_name = %s, user_contact_csv_file_name = %s" % (metadata_csv_file_name, seq_csv_file_name, project_csv_file_name, dataset_csv_file_name, user_contact_csv_file_name))
  
  mysql_util = Mysql_util(host = host_prod, db = to_database)
  print('reading seqs')
  seq_csv_parser = Seq_csv(seq_csv_file_name, mysql_util)
  taxonomy       = Taxonomy(seq_csv_parser.taxa, mysql_util)
  #refhvr_id      = Refhvr_id(seq_csv_parser.refhvr_id, mysql_util)
  sequence       = Sequence(seq_csv_parser.sequences, mysql_util)
  pr = Project(mysql_util)
  dataset = Dataset(mysql_util)
  metadata = Metadata(mysql_util, dataset, pr.project_dict)
  
#   command_line_req_met_list = {  'platform':args.platform, 
#                     'country':args.country,
#                     'fragment':args.fragment,
#                     'domain':args.domain,
#                     'dna_region':args.dna_region,
#                     'env_biome':args.env_biome,
#                     'env_feature':args.env_feature,
#                     'env_material':args.env_material,
#                     'env_package':args.env_package
#                     }
                    
  command_line_req_met_list = {   
                        "target_gene" : args.target_gene,
                        "dna_region" : args.dna_region,
                        "platform" : args.platform,
                        "domain" : args.domain,
                        "geo_loc_name" : args.geo_loc_name, 
                        "env_biome" : args.env_biome,
                        "env_feature" : args.env_feature,
                        "env_material" : args.env_material,
                        "env_package" : args.env_package, 
                        "adapter_sequence" : args.adapter_sequence,
                        "illumina_index" : args.illumina_index,
                        "primer_suite" : args.primer_suite,
                        "run" : args.run
         }
                    
  req_md_okay = True
  for term in command_line_req_met_list:
    if command_line_req_met_list[term] == '' or command_line_req_met_list[term] == 'unknown':
        print()
        print('You left ' + term + " to be 'unknown'")
        if term == 'target_gene':
            mdlist = metadata.target_gene_list
        elif term == 'dna_region':
            mdlist = metadata.dna_region_list
        elif term == 'domain':
            mdlist = metadata.domain_list
        elif term == 'platform':
            mdlist = metadata.sequencing_platform_list
        elif term == 'geo_loc_name':
            mdlist = 'list is too big to print here-- see database'  #metadata.term_list
        elif term == 'env_material' or term == 'env_feature' or term == 'env_biome':
            mdlist = 'term list is too big to print here-- see database'  #metadata.term_list
        elif term == 'env_package':
            mdlist = metadata.package_list
        elif term == 'adapter_sequence':
            mdlist = 'run_key list is too big to print-here -- see database'  #metadata.adapter_sequence_list
        elif term == 'illumina_index':
            mdlist = metadata.illumina_index_list
        elif term == 'primer_suite':
            mdlist = metadata.primer_suite_list  
        elif term == 'run':
            mdlist = 'list is too big to print-here -- see database'
        print('tag is -`'+term + "`; Enter the name not the id")
        print('Choose from the list:',mdlist)
        req_md_okay = False
  if not req_md_okay:
    print(metadata.report)
    ans = input("Do you want to continue? (type 'Y' to continue): ")
    if ans.upper() != 'Y':
        sys.exit()

  
  
  if (args.do_not_insert == False):
        utils.benchmarking(sequence.insert_seq, "Inserting sequences...")
  utils.benchmarking(sequence.get_seq_ids, "get_seq_ids")


  utils.benchmarking(pr.parse_project_csv, "parse_project_csv", project_csv_file_name)

  user = User(pr.contact, user_contact_csv_file_name, mysql_util)
  
  if (args.do_not_insert == False):
        utils.benchmarking(user.insert_user, "insert_user")
  utils.benchmarking(user.get_user_id, "get_user_id")
  
  if (args.do_not_insert == False):
    utils.benchmarking(pr.insert_project, "insert_project", user.user_id)

  utils.benchmarking(pr.get_project_id, "get_project_id")
  utils.benchmarking(pr.make_project_dict, "make_project_dict")

  seq_csv_parser.utils.print_array_w_title(user.user_id, "self.user_id main")
  seq_csv_parser.utils.print_array_w_title(pr.project_id, "pr.project_id main")
  seq_csv_parser.utils.print_array_w_title(pr.project_dict, "pr.project_dict main 1")
  
  
  utils.benchmarking(dataset.parse_dataset_csv, "parse_dataset_csv", dataset_csv_file_name)
  utils.benchmarking(dataset.make_dataset_project_dictionary, "make_dataset_project_dictionary")

  seq_csv_parser.utils.print_array_w_title(pr.project_dict, "pr.project_dict main 2")

  if (args.do_not_insert == False):
    utils.benchmarking(dataset.insert_dataset, "insert_dataset", pr.project_dict)
  utils.benchmarking(dataset.collect_dataset_ids, "collect_dataset_ids", pr.project_dict[args.project])
  utils.benchmarking(dataset.make_all_dataset_id_by_project_dict, "make_all_dataset_id_by_project_dict")

  utils.benchmarking(seq_csv_parser.sequence_pdr_info, "sequence_pdr_info", dataset.dataset_id_by_name_dict, sequence.sequences_w_ids)
  if (args.do_not_insert == False):
    utils.benchmarking(seq_csv_parser.insert_sequence_pdr_info, "insert_sequence_pdr_info")
  utils.benchmarking(taxonomy.parse_taxonomy, "parse_taxonomy")
  utils.benchmarking(taxonomy.get_taxa_by_rank, "get_taxa_by_rank")
  utils.benchmarking(taxonomy.make_uniqued_taxa_by_rank_dict, "make_uniqued_taxa_by_rank_dict")
  
  if (args.do_not_insert == False):
    utils.benchmarking(taxonomy.insert_taxa, "insert_taxa")
  utils.benchmarking(taxonomy.silva_taxonomy, "silva_taxonomy")
  
  if (args.do_not_insert == False):
    utils.benchmarking(taxonomy.insert_silva_taxonomy, "insert_silva_taxonomy")
  utils.benchmarking(taxonomy.get_silva_taxonomy_ids, "get_silva_taxonomy_ids")
  utils.benchmarking(taxonomy.make_silva_taxonomy_id_per_taxonomy_dict, "make_silva_taxonomy_id_per_taxonomy_dict")
  utils.benchmarking(taxonomy.get_all_rank_w_id, "get_all_rank_w_id")
  # utils.print_array_w_title(taxonomy.all_rank_w_id, "taxonomy.all_rank_w_id from main")
  utils.benchmarking(seq_csv_parser.silva_taxonomy_info_per_seq_from_csv, "silva_taxonomy_info_per_seq_from_csv", taxonomy)
  
  if (args.do_not_insert == False):
    utils.benchmarking(seq_csv_parser.insert_silva_taxonomy_info_per_seq, "insert_silva_taxonomy_info_per_seq")
  utils.benchmarking(seq_csv_parser.sequence_uniq_info_from_csv, "sequence_uniq_info_from_csv", sequence.sequences_w_ids)
  
  
  
  if (args.do_not_insert == False):
    utils.benchmarking(seq_csv_parser.insert_sequence_uniq_info, "insert_sequence_uniq_info")
  
  # METADATA
  


#   
  
  utils.benchmarking(metadata.parse_metadata_csv, "parse_metadata_csv", metadata_csv_file_name)
  
  ## TODO Required Metadata should run regardless of if MD was found in old vamps
  if not metadata.metadata_file_fields:
    print("No Metadata Found in file")
    
  utils.benchmarking(metadata.add_names_to_params, "add_names_to_params")
  utils.benchmarking(metadata.add_ids_to_params, "add_ids_to_params")  

  utils.benchmarking(metadata.get_existing_field_names, "get_existing_field_names")
  utils.benchmarking(metadata.get_existing_required_metadata_fields, "get_existing_required_metadata_fields")
  utils.benchmarking(metadata.get_existing_custom_metadata_fields, "get_existing_custom_metadata_fields")

  utils.benchmarking(metadata.prepare_required_metadata, "prepare_required_metadata")
  utils.benchmarking(metadata.required_metadata_for_insert, "required_metadata_for_insert")  
  if (args.do_not_insert == False):

    utils.benchmarking(metadata.insert_required_metadata, "insert_required_metadata")

  utils.benchmarking(metadata.data_for_custom_metadata_fields_table, "data_for_custom_metadata_fields_table")
  if (args.do_not_insert == False):
    utils.benchmarking(metadata.insert_custom_metadata_fields, "insert_custom_metadata_fields")

  if not metadata.custom_metadata_fields_uniqued_for_tbl:
    utils.benchmarking(metadata.get_data_from_custom_metadata_fields, "get_data_from_custom_metadata_fields", pr.project_dict)
  utils.benchmarking(metadata.create_custom_metadata_pr_id_table, "create_custom_metadata_pr_id_table")
  if (args.do_not_insert == False):
    utils.benchmarking(metadata.insert_custom_metadata, "insert_custom_metadata")

      
  print("** Finished ** ",args.project,"--ProjectID:",pr.project_id)
  # import winsound
#   Freq = 2500 # Set Frequency To 2500 Hertz
#   Dur = 1000 # Set Duration To 1000 ms == 1 second
#   winsound.Beep(Freq,Dur)
  os.system('echo -e "\a"')

# TODO: 
# *) make "run all in class" methods in client
# http://blog.michelemattioni.me/2015/01/10/list-intersection-in-python-lets-do-it-quickly/
# *) insert data for MBE_1666G_Bv4 and check counts and taxonomy
# done) args for file names
# done) script for getting csv from vampsdb
# *) add check if not data from vamps_prod
# done) move connection with vamps_prod and csv creation out
# *) use buffer for sequences - hangs mysql
# done) add what tables are should be preuploaded (rank, classifier, env_sample_source)
# *) choose database from a command line
# *) add file names from the command line if not created
# done) add data for _pipe into csv
# *) if rank table is not there - try: and give a miningful error
# *) use public = '1' from args for public in project!
# done) /* 12:34:35 PM local_ruby vamps2 */ INSERT INTO `rank` (`rank_id`, `rank`, `rank_number`) VALUES (NULL, 'orderx', '3');
# done) prepopulate (rank, classifier, env_sample_source), remove custom_metadata 2... and save as schema
# done) combine creation metadata values and then insert required or custom_metadata
# done) change 108 to 119 in the git db
# *) required_metadata_info -4??? see on vampsprod for pr MBE_1666G_Bv4

# *) use more the one project at once

