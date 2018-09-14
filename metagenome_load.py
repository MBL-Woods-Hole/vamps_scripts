#!/usr/bin/env python

import subprocess
import sys, os
import csv
import time
import shutil
import datetime
import argparse
import logging
import pymysql as MySQLdb
today = str(datetime.date.today()) 
#import ConMySQL
ranks = ('domain','phylum','class','orderx','family','genus','species','strain')
domains = ('Archaea','Bacteria','Eukarya','Organelle','Unknown')
import timeit
import time
from collections import defaultdict

    
env_package_dict = {
'10':'air',
'20':'extreme habitat',
'30':'host associated',
'40':'human associated',
'45':'human-amniotic-fluid',
'47':'human-blood',
'43':'human-gut',
'42':'human-oral',
'41':'human-skin',
'46':'human-urine',
'44':'human-vaginal',
'140':'indoor',
'50':'microbial mat/biofilm',
'60':'miscellaneous_natural_or_artificial_environment',
'70':'plant associated',
'80':'sediment',
'90':'soil/sand',
'100':'unknown',
'110':'wastewater/sludge',
'120':'water-freshwater',
'130':'water-marine',
}
    
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
        if (args.do_not_update == False):
            sql += " ON DUPLICATE KEY UPDATE\n"   # IGNORE keyword ignored when this is used
            for field in field_name.split(","):
                field = field.strip()
                sql += field+"=VALUES("+field+"),"
            sql = sql.rstrip(',') # remove last comma
            
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
          self.utils.print_both("FAILED: self.execute_simple_select("+field_name+", "+table_name+", "+where_part+")")
          # self.utils.print_both(sys.exc_info()[0])
          id_result = 0
          #raise

      # self.utils.print_array_w_title(id_result, "=====\nid_result IN get_id")
      return id_result


class Utils:
    def __init__(self):
        self.chunk_split = 100
        self.min_seqs = 50000  #300000
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
      xx = open(file_name, 'rb')
      yy = csv.reader(xx, delimiter = ',')
      print(yy)
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
  def __init__(self, auth_id, user_contact_csv_file_name, mysql_util):
    self.utils      = Utils()
    self.user_contact_file_content = []
    self.user_id    = ""
    self.auth_id    = auth_id
    self.user_contact_csv_file_name = user_contact_csv_file_name

    self.parse_user_contact_csv(self.user_contact_csv_file_name)
    self.user_data = self.utils.search_in_2d_list(self.auth_id, self.user_contact_file_content)
    
  def parse_user_contact_csv(self, user_contact_csv_file_name):
    self.user_contact_file_content = self.utils.read_csv_into_list(user_contact_csv_file_name)[1]
    # self.utils.print_array_w_title(self.user_contact_file_content, "===\nself.user_contact_file_content BBB")

  def insert_user(self):
    #field_list    = "username, email, institution, first_name, last_name, active, security_level, encrypted_password"
    field_list    = "username, email, institution, first_name, last_name, active, security_level, encrypted_password"
    print(self.user_data)
    try:
      insert_values = ', '.join(["'%s'" % key for key in self.user_data[2:]])+",'XXXXXXtemp'"
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
    
    
  def get_user_id(self):
    print('2',self.user_data)
    try:
      
        if self.user == 'admin':
            username = 'admin'
        else:
            username = self.user_data[2]
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

  def insert_project(self, user_id, project,title,description,funding):
    print("user_id, project,title,description,funding",user_id, project,title,description,funding,self.public)
    #project, title, project_description, funding, env_sample_source_id, contact, email, institution = self.project_file_content[0]

    field_list     = "project, title, project_description, rev_project_name, funding, owner_user_id, public, metagenomic"
    insert_values  = ', '.join("'%s'" % key for key in [project, title, description])
    insert_values += ", REVERSE('%s'), '%s', '%s', '%s', '%s'" % (project, funding, user_id, self.public,'1')

    # sql = "INSERT %s INTO %s (%s) VALUES (%s)" % ("ignore", "project", field_list, insert_values)
    # self.utils.print_array_w_title(sql, "sql")

    rows_affected = mysql_util.execute_insert("project", field_list, insert_values)
    self.utils.print_array_w_title(rows_affected, "rows_affected by insert_project")

    self.project_id = mysql_util.get_id("project_id", "project", "WHERE project = '%s'" % (self.project), rows_affected)
    print('XXXX',self.project_id)
    # self.utils.print_array_w_title(self.project_dict, "===\nSSS self.project_dict from insert_project ")
    
  def get_project_id(self):
    self.project_id = mysql_util.get_id("project_id", "project", "WHERE project = '%s'" % self.project)
    
    return self.project_id
    
  def make_project_dict(self):
    self.project_dict[self.project] = self.project_id 
  
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

    def __init__(self, mysql_util, project_dict):
        self.utils = Utils()
        self.dataset_idx = 10
        self.dataset_desc_idx = 4
        self.dataset_id_by_name_dict = {}
        self.cust_metadata_values = {}
        self.custom_metadata_fields =[]
        self.env_package_by_did = {}
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

        print('self.target_gene_list',self.target_gene_list)
        #print('self.dna_region_list',self.dna_region_list)
        #print('self.domain_list',self.domain_list)
        print('self.sequencing_platform_list',self.sequencing_platform_list)
        #print('self.geo_loc_name_list',self.geo_loc_name_list)
        print('self.package_list',self.package_list)
        #print('self.adapter_sequence_list',self.adapter_sequence_list)
        #print('self.illumina_index_list',self.illumina_index_list)
        print('self.primer_suite_list',self.primer_suite_list)
        #print('self.run_list',self.run_list)
        
        self.target_gene_id = self.target_gene_list[0][0]
        self.dna_region_id = self.dna_region_list[0][0]
        self.domain_id = self.domain_list[0][0]
        self.primer_suite_id = self.primer_suite_list[0][0]
        
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

    def find_required_id(self, term, req_name):
        id = 'id_not_found'
        name = 'unknown'
       
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
        
            
    def parse_metadata_csv(self, metadata_csv_file_name):
        print("=" * 20)
        print(metadata_csv_file_name)
        self.metadata_file_fields, self.metadata_file_content = self.utils.read_csv_into_list(metadata_csv_file_name)
        print(self.metadata_file_fields)
        print(self.metadata_file_content)
        
    def insert_dataset(self,pid):
        
        #cus_metadata ={'concentration':27, 'quant_method':28, 'env_sample_source_id':34}  # INDICES in line array
        cus_metadata ={'concentration':27, 'quant_method':28}  # INDICES in line array
        env_sample_idx = 34
        self.env_source_id_from_file = {}
        q = "INSERT into dataset (dataset,dataset_description,project_id) VALUES"
        vals = []
        for lst in self.metadata_file_content:
            
            ds = lst[self.dataset_idx]
            self.cust_metadata_values[ds] = {}
            dd = lst[self.dataset_desc_idx]
            self.env_source_id_from_file[ds] = lst[env_sample_idx]
            for c in cus_metadata:
                self.cust_metadata_values[ds][c]=lst[cus_metadata[c]]
            l = "'"+ds+"','"+dd+"','"+str(pid)+"'"
            
            vals.append(l)
        val_str = "), (".join(vals) 
        mysql_util.execute_insert('dataset','dataset, dataset_description, project_id', val_str)
            #q += "('%s','%s','%s')" % (ds,dd, self.dataset_id)
    def collect_dataset_ids(self, pid):
        
        for lst in self.metadata_file_content:
            ds = lst[self.dataset_idx]
            did = mysql_util.get_id("dataset_id", "dataset", "WHERE dataset = '%s' and project_id = %s" % (ds, pid))
            self.dataset_id_by_name_dict[ds] = did       
    
    def required_metadata_for_insert(self):
        all_required_metadata = []
        field_list_temp       = []
        # all datsets will have the same required metadata to start
        
        
    def create_custom_metadata_pr_id_table(self, pid):    
    
    
    # [(275, 'aux_bec_simulated_nitrate_(um)', 'unknown'), (275, 'depth_end', 'meter'), (275, 'aux_bec_simulated_phosphate_(um)', 'unknown'), (275, 'aux_sunset_hr', 'unknown'), 
   
        field_descriptions  = ""
        table_name          = "custom_metadata_%s" % pid
        id_name             = "%s_id" % (table_name)
        primary_key_field   = "%s int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" % (id_name)
    
        field_descriptions  = primary_key_field + "`dataset_id` int(11) unsigned NOT NULL,\n"
        """AAV:: For common long metadata entries the varchar(128) should get replaced with 'text'        """
        
        fields = []
        for entry in self.custom_metadata_fields_uniqued_for_tbl:
            fields.append("`%s` varchar(128) DEFAULT NULL" % (entry))
        fields = list(set(fields))
        field_descriptions += ',\n'.join(fields)+","
        field_descriptions += """
            UNIQUE KEY dataset_id (dataset_id),
            CONSTRAINT %s_ibfk_1 FOREIGN KEY (dataset_id) REFERENCES dataset (dataset_id) ON UPDATE CASCADE
            """ % (table_name)
        
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

    def insert_required_metadata(self):
        #q = "INSERT into required_metadata_info (dataset_id,target_gene_id,dna_region_id,platform_id, domain_id,geo_loc_name_id,env_biome_id,env_feature_id,env_material_id,env_package_id,adapter_sequence_id,illumina_index_id,primer_suite_id,run_id)        
        #field_list = "dataset_id,target_gene_id,dna_region_id,platform_id, domain_id,geo_loc_name_id,env_biome_id,env_feature_id,env_material_id,env_package_id,adapter_sequence_id,illumina_index_id,primer_suite_id,run_id"
        field_list=[]
        field_list2 = ['target_gene_id','dna_region_id','sequencing_platform_id','domain_id','geo_loc_name_id','env_biome_id','env_feature_id','env_material_id','env_package_id','adapter_sequence_id','illumina_index_id','primer_suite_id','run_id']
        command_line_req_met_list = {   
                        "target_gene_id" : self.target_gene_id,
                        "dna_region_id" : self.dna_region_id,
                        "sequencing_platform_id" : self.sequencing_platform_id,
                        "domain_id" : self.domain_id,
                        "geo_loc_name_id" : self.geo_loc_name_id, 
                        "env_biome_id" : self.env_biome_id,
                        "env_feature_id" : self.env_feature_id,
                        "env_material_id" : self.env_material_id,
                        "env_package_id" : self.env_package_id, 
                        "adapter_sequence_id" : self.adapter_sequence_id,
                        "illumina_index_id" : self.illumina_index_id,
                        "primer_suite_id" : self.primer_suite_id,
                        "run_id" : self.run_id
                    }
        
        for field in field_list2:
             field_list.append(field)
        fld_list = 'dataset_id'+","+','.join(field_list)   
        vals = []
        for ds in self.dataset_id_by_name_dict:
            print('self.env_source_id_from_file',self.env_source_id_from_file)
            
            env_sample_source = env_package_dict[self.env_source_id_from_file[ds]]  # eg 20 => 'sediment'
            
            (env_package,self.env_package_id)                = self.find_required_id('env_package', env_sample_source)
           
            command_line_req_met_list['env_package_id'] = self.env_package_id
            did = self.dataset_id_by_name_dict[ds]
            l = "'"+str(did)+"'"
            for field in field_list2:
                l += ",'"+command_line_req_met_list[field]+"'"
            vals.append(l)    
        val_str = "), (".join(vals)
        mysql_util.execute_insert('required_metadata_info',fld_list,val_str)
        
    def gather_custom_metadata(self):
        print(self.cust_metadata_values)
        n = 0
        for ds in self.cust_metadata_values:
            if n==0:
                for field in self.cust_metadata_values[ds]:
                    print(field)
                    self.custom_metadata_fields.append(field)
                    self.custom_metadata_fields_uniqued_for_tbl.append(field)
            n +=1
    def insert_custom_metadata(self,pid):
        # TODO: simplify
        #self.make_custom_metadata_per_project_dataset_dict()
        field_str = "dataset_id, "  + "`"   + "`, `".join(self.custom_metadata_fields)   + "`"
        #for project_id, custom_metadata_dict_per_dataset in self.custom_metadata_per_project_dataset_dict.items():
        custom_metadata_table_name = "custom_metadata_%s" % pid
        insert_values              = ""
        vals = []  
        print(field_str)
        
        for ds in self.cust_metadata_values:
            #{'WashingtonMargin_GC4_150cmbsf': {'concentration': '1.0000', 'quant_method': 'nanodrop', 'env_sample_source_id': '20'}
            
            did = self.dataset_id_by_name_dict[ds]
            
            #self.env_package_by_did[did] = env_package[self.cust_metadata_values[ds]['env_sample_source_id']]
            
            l = "'"+str(did)+"'"
            for field in self.custom_metadata_fields: # keep the correct order
                l += ",'"+self.cust_metadata_values[ds][field]+"'"
            vals.append(l)
        val_str = "), (".join(vals)
                
        rows_affected = mysql_util.execute_insert(custom_metadata_table_name, field_str, val_str)
        self.utils.print_array_w_title(rows_affected, "rows affected by insert_custom_metadata")     
    
    def insert_custom_fields(self,pid):        
        field_str = "project_id,field_name,field_units,example"   
        vals = []
        n = 0
        for ds in self.cust_metadata_values:
            if n==0:
                for field in self.cust_metadata_values[ds]:
                    vals.append("'"+str(pid)+"','"+field+"','Alphanumeric','"+self.cust_metadata_values[ds][field]+"'")
        # for field in self.custom_metadata_fields:
#               vals.append("'"+str(pid)+"','"+field+"','Alphanumeric',''")
        val_str = "), (".join(vals)
        mysql_util.execute_insert('custom_metadata_fields', field_str, val_str)
        
if __name__ == '__main__':
    
    
    # DEFAULTS
    site = 'vampsdev'

    data_object = {}

    
    myusage = """
    ./metagenome_load.py 
    
        -host vampsdev          []
        -p DCO_GRAW_Sgun        []
    
        --do_not_insert
    
        --sequencing_platform   [illumina]
        --geo_loc_name
        --target_gene           [ shotgun ]
        --domain                [ shotgun ]
        --dna_region            [ shotgun ]
        --env_biome
        --env_feature
        --env_material
        --env_package 
        --run_key
        --illumina_index
        --primer_suite          [ Shotgun Suite ]
        --run
    
    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)                 
      
       
    parser.add_argument("-p", "--project",    required=True,  action='store', dest = "project",  help="")   
    parser.add_argument("-host", "--host",    required=True, action = "store", dest = "host", 
        help = """Site where the script is running""")
    parser.add_argument("-ni","--do_not_insert",
      required = False, action = "store_true", dest = "do_not_insert", default = False,
      help = """Do not insert data into db, mostly for debugging purposes""")                                                                                                                                                      
    # Required Metadata
    parser.add_argument("-platform","--sequencing_platform",
      required = False, action = "store", dest = "platform", default = 'illumina',
      help = """Sequencing platform -- must match text in newVAMPS Table""")
    parser.add_argument("-geo_loc_name","--geo_loc_name",
      required = False, action = "store", dest = "geo_loc_name", default = '',
      help = """geo_loc_name -- must match text in newVAMPS Table""")    
    parser.add_argument("-target_gene", "--target_gene",
        required = False, action = "store", dest = "target_gene", default='metagenome',
        help = """target_gene Name -- no entry results in 'unknown' """)
    parser.add_argument("-domain", "--domain",
        required = False, action = "store", dest = "domain", default='shotgun',
        help = """Domain -- no entry results in 'unknown' """)   
    parser.add_argument("-dna_region", "--dna_region",
        required = False, action = "store", dest = "dna_region", default='random',
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
        required = False, action = "store", dest = "primer_suite", default='Shotgun Suite',
        help = """primer_suite -- no entry results in 'unknown' """) 
    parser.add_argument("-run", "--run",
        required = False, action = "store", dest = "run", default='',
        help = """run -- no entry results in 'unknown' """) 
    args = parser.parse_args()
    args.public = '0'
    user_csv_file_name = "user_contact_%s.csv" % (args.project)
    project_csv_file_name      = "project_%s.csv" % (args.project)
    metadata_csv_file_name     = "metadata_%s.csv" % (args.project)
    args.do_not_update = True
    # ========

    print("metadata_csv_file_name = %s,  project_csv_file_name = %s,  user_csv_file_name = %s" % (metadata_csv_file_name,  project_csv_file_name, user_csv_file_name))
    host_prod   = "bpcweb7"
    to_database = 'vamps2'
    if args.host == 'vamps' or args.host == 'vampsdb':
        host_prod = "vampsdb"
    else:
        host_prod = "bpcweb7"
    
    utils = Utils()
    # 
    read_default_file_prod = "~/.my.cnf"
    port_prod = 3306
    mysql_util = Mysql_util(host = host_prod, db = 'vamps2')
    
    pr = Project(mysql_util)
    pr.parse_project_csv(project_csv_file_name)
    project_file_content = pr.project_file_content[0]
    pr_auth_id = project_file_content[2]
    pr_title = project_file_content[4]
    pr_desc = project_file_content[5]
    print('auth_id',pr_auth_id, 'Owner Line:',project_file_content)
    
    user = User(pr_auth_id, user_csv_file_name, mysql_util)
    print(user)
    
    print(pr.project_dict)
    
    #metadata = Metadata(mysql_util, dataset, pr.project_dict)
    metadata = Metadata(mysql_util, pr.project_dict)
    
    # DEFAULTS
    #args.infile_suffix = 'fa.unique.spingo.out'
    #args.env_source_id = '100'   # unknown
    args.title = 'mytitle'
    args.description ='mydescription'
    args.contact = 'Ad Min'
    args.email = 'vamps@mbl.edu'
    args.institution = 'MBL'
    args.default_req_metadata = {
        "env_biome":"unknown",
        "env_feature":"unknown",
        "env_material":"unknown",
        "env_package":"unknown",
        "collection_date":"",
        "latitude":"",
        "longitude":"",
        "target_gene":"unknown",
        "dna_region":"unknown",
        "sequencing_platform":"unknown",
        "domain":"unknown",
        "geo_loc_name":"unknown",
        "adapter_sequence":"unknown",
        "illumina_index":"unknown",
        "primer_suite":"unknown",
        "run":"unknown"
    }
    
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
                mdlist = 'list is too big to print-- here -- see database'  #metadata.term_list
            elif term == 'env_material' or term == 'env_feature' or term == 'env_biome':
                mdlist = 'term list is too big to print-- here -- see database'  #metadata.term_list
            elif term == 'env_package':
                mdlist = metadata.package_list
            elif term == 'adapter_sequence':
                mdlist = 'run_key list is too big to print-- here -- see database'  #metadata.adapter_sequence_list
            elif term == 'illumina_index':
                mdlist = metadata.illumina_index_list
            elif term == 'primer_suite':
                mdlist = metadata.primer_suite_list  
            elif term == 'run':
                mdlist = 'list is too big to print-- here -- see database'    
            print('tag is -'+term + "; Enter the name not the id")
            print('Choose from the list:',mdlist)
            req_md_okay = False
    if not req_md_okay:
        print(metadata.report)
        ans = raw_input("Do you want to continue? (type 'Y' to continue): ")
        if ans.upper() != 'Y':
            sys.exit()
            
   

    

    if (args.do_not_insert == False):
        utils.benchmarking(user.insert_user, "insert_user")
    utils.benchmarking(user.get_user_id, "get_user_id")
    
    if (args.do_not_insert == False):
        utils.benchmarking(pr.insert_project, "insert_project", user.user_id, args.project,pr_title,pr_desc,'unknown')

    #utils.benchmarking(pr.get_project_id, "get_project_id")
    utils.benchmarking(pr.make_project_dict, "make_project_dict")

    #Dataset from metadata file
    #utils.benchmarking(dataset.parse_dataset_csv, "parse_dataset_csv", dataset_csv_file_name)
    #utils.benchmarking(dataset.make_dataset_project_dictionary, "make_dataset_project_dictionary")

    

#     if (args.do_not_insert == False):
#         utils.benchmarking(dataset.insert_dataset, "insert_dataset", pr.project_dict)
#     utils.benchmarking(dataset.collect_dataset_ids, "collect_dataset_ids", pr.project_dict[args.project])
#     utils.benchmarking(dataset.make_all_dataset_id_by_project_dict, "make_all_dataset_id_by_project_dict")

    # METADATA
   
    #   sys.exit('Skipping Metadata for Testing (remove this if on production)')
    #   

    utils.benchmarking(metadata.parse_metadata_csv, "parse_metadata_csv", metadata_csv_file_name)
    print('pid',pr.project_id)
    
    metadata.insert_dataset(pr.project_id)
    metadata.collect_dataset_ids(pr.project_id)
    print(metadata.dataset_id_by_name_dict)
    
    ## TODO Required Metadata should run regardless of if MD was found in old vamps
    if not metadata.metadata_file_fields:
        print("No Metadata Found in file")

    #utils.benchmarking(metadata.add_names_to_params, "add_names_to_params")
    #utils.benchmarking(metadata.add_ids_to_params, "add_ids_to_params")  
    #utils.benchmarking(metadata.get_existing_field_names, "get_existing_field_names")
    # utils.benchmarking(metadata.get_existing_required_metadata_fields, "get_existing_required_metadata_fields")
#     utils.benchmarking(metadata.get_existing_custom_metadata_fields, "get_existing_custom_metadata_fields")
#     utils.benchmarking(metadata.prepare_required_metadata, "prepare_required_metadata")
    utils.benchmarking(metadata.required_metadata_for_insert, "required_metadata_for_insert") 
     
    if (args.do_not_insert == False):
        utils.benchmarking(metadata.insert_required_metadata, "insert_required_metadata")
        utils.benchmarking(metadata.gather_custom_metadata, "gather_custom_metadata")
        metadata.create_custom_metadata_pr_id_table(pr.project_id) 
        metadata.insert_custom_metadata(pr.project_id) 
        metadata.insert_custom_fields(pr.project_id)
   #  utils.benchmarking(metadata.data_for_custom_metadata_fields_table, "data_for_custom_metadata_fields_table")
#     
#     if (args.do_not_insert == False):
#         utils.benchmarking(metadata.insert_custom_metadata_fields, "insert_custom_metadata_fields")
# 
#     if not metadata.custom_metadata_fields_uniqued_for_tbl:
#         utils.benchmarking(metadata.get_data_from_custom_metadata_fields, "get_data_from_custom_metadata_fields", pr.project_dict)
#     utils.benchmarking(metadata.create_custom_metadata_pr_id_table, "create_custom_metadata_pr_id_table")
#     
#     if (args.do_not_insert == False):
#         utils.benchmarking(metadata.insert_custom_metadata, "insert_custom_metadata")
  
    print("** Finished ** ",args.project,"--ProjectID:",pr.project_id)
    # import winsound
    #   Freq = 2500 # Set Frequency To 2500 Hertz
    #   Dur = 1000 # Set Duration To 1000 ms == 1 second
    #   winsound.Beep(Freq,Dur)
    os.system('echo -e "\a"')
