#!/usr/bin/env python

##!/usr/bin/env python
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

import os
from stat import * # ST_SIZE etc
import sys
import shutil
import types
import time
import random
import csv
from time import sleep
import configparser as ConfigParser
from IlluminaUtils.lib import fastalib
import datetime
import logging
import subprocess
import pymysql as MySQLdb
import unicodedata
import pprint

today = str(datetime.date.today())
pp = pprint.PrettyPrinter(indent=4)

# Global:
REQ_METADATA_ITEMS = {}
CUST_METADATA_ITEMS = {}
DATASETS = {}
PROJECTID = 0
required_metadata_fields = [  "collection_date","env_biome", "env_feature", "env_material", "env_package","geo_loc_name","latitude", "longitude", "dna_region",'adapter_sequence','sequencing_platform','target_gene','domain','illumina_index','primer_suite', 'run'];
required_id_metadata_fields= [  "env_biome", "env_feature", "env_material","env_package","geo_loc_name", "dna_region",'adapter_sequence','sequencing_platform','target_gene','domain','illumina_index','primer_suite', 'run' ];



LOG_FILENAME = os.path.join('.','convert_old_vamps_project.log')
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILENAME, filemode="w",
                           format="%(asctime)-15s %(levelname)-8s %(message)s")
#logging = logging.getlogging('')
#os.chdir(args.indir)






def put_required_metadata(args):


    #q_req = "INSERT into required_metadata_info (dataset_id,"+','.join(required_metadata_fields)+")"
    #q_req = q_req+" VALUES('"
    cursor = args.db.cursor()
    #for i,did in enumerate(REQ_METADATA_ITEMS['dataset_id']):
    for ds in DATASETS:
        did = DATASETS[ds]
        vals = [str(did)]
        fields=[]
        for key in required_metadata_fields:
            if did in REQ_METADATA_ITEMS and key in REQ_METADATA_ITEMS[did]:
                vals.append(REQ_METADATA_ITEMS[did][key])
                fields.append(key)

        f = ",".join(fields)
        v = "','".join(vals)
        q_req = "INSERT into required_metadata_info (dataset_id,"+f+")"
        q_req = q_req+" VALUES('"

        q2_req = q_req + v + "')"
        logging.debug( q2_req)
        print(q2_req)
        try:
            cursor.execute(q2_req)

        except MySQLdb.Error(e):
            try:
                logging.debug("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            except IndexError:
                logging.debug("MySQL Error: %s" % str(e))



    args.db.commit()

def put_custom_metadata(args):
    """
      create new table
    """

    print ("put_custom_metadata: CUST_METADATA_ITEMS")
    

    cursor = args.db.cursor()
    # TABLE-1 === custom_metadata_fields
    cust_keys_array = {}
    all_cust_keys = []  # to create new table
   
    for ds in DATASETS:
        did = DATASETS[ds]
        cust_keys_array[did]=[]
        

        if did in CUST_METADATA_ITEMS:
            print ("did in CUST_METADATA_ITEMS")
            print (did)
            for key in CUST_METADATA_ITEMS[did]:
                if key not in all_cust_keys:
                    all_cust_keys.append(key)
                if key not in cust_keys_array[did]:
                    cust_keys_array[did].append(key)
                # q2 = "INSERT IGNORE into custom_metadata_fields(project_id, field_name, field_type, example)"
                q2 = "INSERT IGNORE into custom_metadata_fields(project_id, field_name,  example)"
                q2 += " VALUES("
                q2 += "'"+str(PROJECTID)+"',"
                q2 += "'"+str(key)+"',"
                
                q2 += "'"+str(CUST_METADATA_ITEMS[did][key])+"')"
                print ("put_custom_metadata: q2")
                print (q2)

                
                cursor.execute(q2)
        args.db.commit()


    # TABLE-2 === CREATE custom_metadata_<pid>
    custom_table = 'custom_metadata_'+str(PROJECTID)
    q = "CREATE TABLE IF NOT EXISTS `"+ custom_table + "` (\n"
    q += " `"+custom_table+"_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
    # q += " `project_id` int(11) unsigned NOT NULL,\n"
    q += " `dataset_id` int(11) unsigned NOT NULL,\n"
    
    for key in all_cust_keys:
        if key != 'dataset_id':
            q += " `"+key+"` varchar(128) DEFAULT NULL,\n"
    q += " PRIMARY KEY (`"+custom_table+"_id` ),\n"
    # unique_key = "UNIQUE KEY `unique_key` (`project_id`,`dataset_id`,"
    unique_key = "UNIQUE KEY `unique_key` (`dataset_id`,"

    # ONLY 16 key items allowed:
    for i,key in enumerate(all_cust_keys):
        if i < 14 and key != 'dataset_id':
            unique_key += " `"+key+"`,"
    q += unique_key[:-1]+"),\n"
    # q += " KEY `project_id` (`project_id`),\n"
    q += " KEY `dataset_id` (`dataset_id`),\n"
    # q += " CONSTRAINT `"+custom_table+"_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `project` (`project_id`) ON UPDATE CASCADE,\n"
    q += " CONSTRAINT `"+custom_table+"_ibfk_2` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`dataset_id`) ON UPDATE CASCADE\n"
    q += " ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
    print(q)
    cursor.execute(q)


    # add data
    for ds in DATASETS:
        did = DATASETS[ds]
        # q3 = "INSERT into "+custom_table+" (project_id,dataset_id,"
        q3 = "INSERT into "+custom_table+" (dataset_id,"
        for key in cust_keys_array[did]:
            logging.debug("key in cust_keys_array[did] = ")
            logging.debug(key)
            if key != 'dataset_id':
                q3 += "`"+key+"`,"
        q3 = q3[:-1]+ ")"
        # q3 += " VALUES('"+str(CONFIG_ITEMS['project_id'])+"','"+str(did)+"',"
        q3 += " VALUES('"+str(did)+"',"
        for key in cust_keys_array[did]:
            if key != 'dataset_id':
                if key in CUST_METADATA_ITEMS[did]:
                    q3 += "'"+str(CUST_METADATA_ITEMS[did][key])+"',"
        q3 = q3[:-1] + ")"
        print (q3)
        cursor.execute(q3)

    args.db.commit()
#

# def get_metadata(args, DATASET_ID_BY_NAME):
#     logging.debug('csv '+str(args.metadata_file))
#     if args.delim == 'comma':
#         lines = list(csv.reader(open(args.metadata_file, 'rb'), delimiter=','))
#     else:
#         lines = list(csv.reader(open(args.metadata_file, 'rb'), delimiter='\t'))
# 
#     TMP_METADATA_ITEMS = {}
#     for line in lines:
#         #print line
#         if not line:
#             continue
#         if line[0] == 'dataset' and line[1] == 'parameterName':
#             headers = line
#         else:
#             key = line[7].replace(' ','_').replace('/','_').replace('+','').replace('(','').replace(')','').replace(',','_').replace('-','_').replace("'",'').replace('"','').replace('<','&lt;').replace('>','&gt;')   # structured comment name
#             if key == 'lat':
#                 key='latitude'
#             if key == 'lon' or key == 'long':
#                 key='longitude'
#             parameterValue = remove_accents(line[2])
#             dset = line[0]
#             pj = line[5]
#             if dset in TMP_METADATA_ITEMS:
#                 TMP_METADATA_ITEMS[dset][key] = parameterValue
#             else:
#                 TMP_METADATA_ITEMS[dset] = {}
#                 TMP_METADATA_ITEMS[dset][key] = parameterValue
# 
# 
#     # now get the data from just the datasets we have in CONFIG.ini
#     for ds in CONFIG_ITEMS['datasets']:
#         #print ds
#         try:
#           did = str(DATASET_ID_BY_NAME[ds])
#         # except KeyError:
#         #   insert_dataset(CONFIG_ITEMS, args)
#         except:
#           raise
#           
#         if ds in TMP_METADATA_ITEMS:
#             for key in TMP_METADATA_ITEMS[ds]:
#                 #print key
# 
#                 if key in required_metadata_fields:
#                     if did in REQ_METADATA_ITEMS:
#                         REQ_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')
#                     else:
#                         REQ_METADATA_ITEMS[did]= {}
#                         REQ_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')
# 
#                 else:
# 
#                     if did in CUST_METADATA_ITEMS:
#                         CUST_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')
#                     else:
#                         CUST_METADATA_ITEMS[did]= {}
#                         CUST_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')
# 
# 
# 



def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', unicode(input_str.strip(), 'utf8'))
    res = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # print "res = "
    # print res
    return res
    

    

        
# class Old_vamps_data:
#   """
#     get data from csv files made from old vamps
#     put data to vamps2
#   """
#   def __init__(self, db):
#     self.mysql_util = Mysql_util(host = 'localhost', db="vamps2")
#     
#     # self.mysql_util = Mysql_util(mysql_conn)
#     self.cursor = db.cursor()
#     self.dataset_id_by_name_dict = {}
#     self.project_id_by_name_dict = {}
#     self.make_dataset_by_name_dict()
#     self.make_project_by_name_dict()
#         
#   def make_dataset_by_name_dict(self):
#     datasets_w_ids = self.mysql_util.get_all_name_id('dataset')
#     self.dataset_id_by_name_dict = dict(datasets_w_ids)
# 
#   def make_project_by_name_dict(self):
#     projects_w_ids = self.mysql_util.get_all_name_id('project')
#     self.project_id_by_name_dict = dict(projects_w_ids)
# 
#   def collect_datasets(self, seqs_file_lines):
#     # datasets_w_ids
#       # logging.debug("In collect_datasets, seqs_file_lines = ")
#       # logging.debug(seqs_file_lines)
#       CONFIG_ITEMS_datasets_set = set()
#       for field_list in seqs_file_lines:
#         # print "field_list[3] = "
#         # print field_list[3]
#         CONFIG_ITEMS_datasets_set.add(field_list[3])
#       CONFIG_ITEMS['datasets'] = list(CONFIG_ITEMS_datasets_set)
#       # print "In collect_datasets, CONFIG_ITEMS['datasets'] = %s" % CONFIG_ITEMS['datasets']
# 
#       # [['id', 'sequence', 'project', 'dataset', 'taxonomy', 'refhvr_ids', 'rank', 'seq_count', 'frequency', 'dis     17 tance', 'rep_id', 'project_dataset']
# 
#   def put_custom_metadata_a(self, CUST_METADATA_ITEMS):
#       """
#         create new table
#       """
#       logging.debug('starting put_custom_metadata')
#       # TABLE-1 === custom_metadata_fields
#       cust_keys_array = {}
#       all_cust_keys = []  # to create new table
#       logging.debug("CONFIG_ITEMS['datasets'] = ")
#       logging.debug(CONFIG_ITEMS['datasets'])
#       print "CUST_METADATA_ITEMS = "
#       print CUST_METADATA_ITEMS
#       for ds in CONFIG_ITEMS['datasets']:
#           did = str(self.dataset_id_by_name_dict[ds])
#           logging.debug("DATASET_ID_BY_NAME[ds] = ")
#           logging.debug(did)
# 
#           cust_keys_array[did]=[]
# 
#           # if did in CUST_METADATA_ITEMS:
#           try:
#             for key in CUST_METADATA_ITEMS[did]:
#                 logging.debug("key in CUST_METADATA_ITEMS[did] = ")
#                 logging.debug(key)
#                 if key not in all_cust_keys:
#                     all_cust_keys.append(key)
#                 if key not in cust_keys_array[did]:
#                     cust_keys_array[did].append(key)
#                 # q2 = "INSERT IGNORE into custom_metadata_fields(project_id, field_name, field_type, example)"
#                 q2 = "INSERT IGNORE into custom_metadata_fields (project_id, field_name, field_type, example)"
#                 q2 += " VALUES("
#                 q2 += "'1',"
#                 # should be:
#                 # q2 += "'"+str(CONFIG_ITEMS['project_id'])+"',"
#                 """
#                 todo
#                 q2 += "'"+str(CONFIG_ITEMS['project_id'])+"',"
#                 make dict for all
#                 if args.add_project:
# 
#                     q = "SELECT project_id from project where project='%s'" % (args.project)
#                     logging.debug(q)
#                     cur.execute(q)
#                     mysql_conn.commit()
#                     row = cur.fetchone()
#                     CONFIG_ITEMS['project_id'] = row[0]
#                     print("ADD TO PID="+str(CONFIG_ITEMS['project_id']))
#                     logging.debug("ADDING to project -- PID="+str(CONFIG_ITEMS['project_id']))
# 
# 
#                 """
#                 q2 += "'"+str(key)+"',"
#                 q2 += "'varchar(128)'," #? are they alvays the same? couldn't they by numbers?
#                 q2 += "'"+str(CUST_METADATA_ITEMS[did][key])+"')"
#                 logging.debug("q2 = ")
#                 logging.debug(q2)
#                 print "q2 = "
#                 print q2
#                 self.cursor.execute(q2)
#           except:
#             raise
          
#===================================          
def get_pids(args, dsets):
    global PROJECTID
    sql = "select dataset,dataset_id,project_id from dataset join project using (project_id) where project='%s'"
    q = sql % (args.project)
    print (q)
    cursor = args.db.cursor()
    cursor.execute(q)
    rowcount = cursor.rowcount
    obj = {}
    if rowcount == 0:
        print ('no datasets found -- exiting')
        sys.exit()
    rows = cursor.fetchall()
    for row in rows:
        obj[row[0]]=row[1]
        PROJECTID = row[2]
    return obj
    
    
    
def read_vamps_metadata(args):
    # "dataset","parameterName","parameterValue","units","miens_units","project","units_id","structured_comment_name","method","other","notes","ts","entry_date","parameter_id","project_dataset"
    # "VCR_1_1B","lat_lon","+37.44491667-75.83458333","+/-decimalDegree+/-decimalDegree","+/-decimalDegree+/-decimalDegree","LTR_VCR_Bv6","0","lat_lon","unknown","0","PRN 2010_07_14_15_14_33  version 1 of phase3 --/tmp/VCR_env_data.txt","2012-04-27 08:25:07","2010-07-14","0","LTR_VCR_Bv6--VCR_1_1B"

    print('csv '+str(args.infile))
    print('Reading with `'+args.delim+'` delimiter')
    if args.delim == 'comma':
        
        lines = list(csv.reader(open(args.infile, 'r'), delimiter=','))
    else:
        lines = list(csv.reader(open(args.infile, 'r'), delimiter='\t'))
    dataset_lookup = {}
    TMP_METADATA_ITEMS = {}
    for line in lines:
        print (line)
        if not line:
            continue
        l = [x.strip('"').strip("'") for x in line[0].split(',')]
        print (l)
        if l[0] == 'dataset' and l[1] == 'parameterName':
            headers = line
            print ('got header')
            
        else:
            key = l[7].replace(' ','_').replace('/','_').replace('+','').replace('(','').replace(')','').replace(',','_').replace('-','_').replace("'",'').replace('"','').replace('<','&lt;').replace('>','&gt;')   # structured comment name
            if key == 'lat':
                key='latitude'
            if key == 'lon' or key == 'long':
                key='longitude'
            parameterValue = remove_accents(l[2])
            dset = l[0]
            dataset_lookup[dset] = 1
            pj = l[5]
            if dset in TMP_METADATA_ITEMS:
                TMP_METADATA_ITEMS[dset][key] = parameterValue
            else:
                TMP_METADATA_ITEMS[dset] = {}
                TMP_METADATA_ITEMS[dset][key] = parameterValue
    
    push_metadata_into_objects(args, dataset_lookup, TMP_METADATA_ITEMS)
        


def read_mobe_metadata(args):
    # sample_name    	barcode	center_name    	center_project_name    	emp_status     	experiment_center      	experiment_design_description  	experiment_title       	illumina_technology    	library_construction_protocol  	linker 	pcr_primers      	platform       	primer 	run_center     	run_date       	run_prefix     	samp_size      	sample_center  	sequencing_meth	study_center   	target_gene    	target_subfragment
    # 10091.0.12     	AACCAAGG       	UC Davis_Dave Mills Lab	Secretor Project       	NOT_EMP	UCDCAES	Secretor Status Davis  	Secretor Project       	MiSeq  	amplified with barcoded primers targeting V4 16S rRNA gene, sent to UC Davis Genome Center for Illumina adapters to be ligated on and sequenced on Illumina MiSeq	GT     	FWD:GTGCCAGCMGCCGCGGTAA; REV:GGACTACHVGGGTWTCTAAT      	Illumina       	GTGCCAGCMGCCGCGGTAA    	UCDGC  	8/15/12	Mills081512_S1_L001_R1_001     	200ml    	UC Davis       	Sequencing by synthesis	UCD_Mills_lab  	16S    	V4
    print('csv '+str(args.infile))
    print('Reading with `'+args.delim+'` delimiter')
    if args.delim == 'comma':
        lines = list(csv.reader(open(args.infile, 'r'), delimiter=','))
    else:
        lines = list(csv.reader(open(args.infile, 'r'), delimiter='\t'))
    metadata = {}
    dataset_lookup = {}
    TMP_METADATA_ITEMS = {}
    for line in lines:
        
        if not line:
            continue
        print (line)
        l = [x.strip('"').strip("'") for x in line[0].split(',')]
        print ('l',l)
        if l[0] == 'sample_name' or l[0] == 'SampleID' or l[0][1:] == 'SampleID':
            headers = line
            print ('got header',headers)
            
        else:
            dset = line[0]
            print ('dset',dset)
            dataset_lookup[dset] = 1
            
            for j,val in enumerate(line[1:]):
                key = headers[j+1]
                #print j,dset,key,val
                if dset in TMP_METADATA_ITEMS:
                    TMP_METADATA_ITEMS[dset][key] = val
                else:
                    TMP_METADATA_ITEMS[dset] = {}
                    TMP_METADATA_ITEMS[dset][key] = val
        
    push_metadata_into_objects(args, dataset_lookup, TMP_METADATA_ITEMS)
    
def push_metadata_into_objects(args, dataset_lookup, metadata_obj):
    global DATASETS
    DATASETS = get_pids(args, dataset_lookup)
    print ('datasets from file',     metadata_obj.keys()   )
    print ('datasets from database', DATASETS.keys() )
    # now get the data from just the datasets we have in CONFIG.ini
    for ds in DATASETS:
        
        #print ds
        did = DATASETS[ds]
        #print did  
        if ds in metadata_obj:
            
            for key in metadata_obj[ds]:
                #print key
                if key in required_metadata_fields:
                    if did in REQ_METADATA_ITEMS:
                        REQ_METADATA_ITEMS[did][key] = metadata_obj[ds][key].replace('"','').replace("'",'')
                    else:
                        REQ_METADATA_ITEMS[did]= {}
                        REQ_METADATA_ITEMS[did][key] = metadata_obj[ds][key].replace('"','').replace("'",'')
                else:
                    if did in CUST_METADATA_ITEMS:
                        CUST_METADATA_ITEMS[did][key] = metadata_obj[ds][key].replace('"','').replace("'",'')
                    else:
                        CUST_METADATA_ITEMS[did]= {}
                        CUST_METADATA_ITEMS[did][key] = metadata_obj[ds][key].replace('"','').replace("'",'')
        else:
            print (ds,'not found in metadata_obj')

    
if __name__ == '__main__':
    import argparse
    myusage = """
        -p/--project  project name          REQUIRED

        -m/--metadata_file   metadata file  REQUIRED --FORMAT: see below

        -delim/--delimiter   comma or [DEFAULT:]tab
        
        -host/--host                      REQUIRED  (vamps or vampsdev)
        

    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)

    parser.add_argument("-p","--project",
                required=True,  action="store",   dest = "project", default='',
                help="""Project Name""")

    parser.add_argument("-m","--metadata_file",
                required=True,  action="store",   dest = "infile", default='',
                help="""file path""")
    
    parser.add_argument("-style","--metadata_file_style",
                required=False,  action="store",   dest = "style", default='vamps',
                help="""[mobe OR vamps]""")
                
    parser.add_argument("-delim","--delimiter",
                required=False,  action="store",   dest = "delim", default='tab',
                help="""METADATA: comma or tab""")
    
    parser.add_argument("-host", "--host",
        required = True, action = "store", dest = "host", 
        help = """Site where the script is running""")
    
    if len(sys.argv[1:]) == 0:
        print (myusage)
        sys.exit() 
    args = parser.parse_args()
          
    if args.host == 'vamps' or args.host == 'vampsdb':
        dbhost = 'vampsdb'
        args.sqldb = 'vamps2'
    elif args.host == 'vampsdev':
        dbhost = 'vampsdev'
        args.sqldb = 'vamps2'
    else:
        dbhost = 'localhost'
        args.sqldb = 'vamps_development'
    

    args.db = MySQLdb.connect(host=dbhost, db=args.sqldb, # your host, usually localhost
                             read_default_file="~/.my.cnf_node"  )
    if args.project and args.infile:   
        if args.style == 'mobe':
            read_mobe_metadata(args)
        else:
            read_vamps_metadata(args)

        print ('REQ_METADATA_ITEMS:',REQ_METADATA_ITEMS)
        print ('CUST_METADATA_ITEMS:',CUST_METADATA_ITEMS)
        print ('DATASETS:',DATASETS)
        print ('PROJECTID:',PROJECTID)
        sys.exit()
        if len(REQ_METADATA_ITEMS) == 0 and len(CUST_METADATA_ITEMS) == 0 :
            print ('NO DATA -- Exiting')
            sys.exit() 
        if len(REQ_METADATA_ITEMS) > 0:
            put_required_metadata(args)
        if len(CUST_METADATA_ITEMS) > 0:
            put_custom_metadata(args)
             
    else:
        print (myusage)


