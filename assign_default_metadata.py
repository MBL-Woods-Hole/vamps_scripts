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
import logging
import csv
from time import sleep

import datetime
today = str(datetime.date.today())
import subprocess
import pymysql as MySQLdb

"""

"""
# Global:
#NODE_DATABASE = "vamps_js_dev_av"
#NODE_DATABASE = "vamps_js_development"
CONFIG_ITEMS = {}
DATASET_ID_BY_NAME = {}
REQ_METADATA_ITEMS = {}
CUST_METADATA_ITEMS = {}

required_metadata_fields = [  "collection_date","env_biome", "env_feature", "env_material", "env_package","geo_loc_name","latitude", "longitude", "dna_region",'adapter_sequence','sequencing_platform','target_gene','domain','illumina_index','primer_suite', 'run'];
required_id_metadata_fields= [  "env_biome", "env_feature", "env_material","env_package","geo_loc_name", "dna_region",'adapter_sequence','sequencing_platform','target_gene','domain','illumina_index','primer_suite', 'run' ];
#required_id_metadata_fields= [  "env_biome_id", "env_feature_id", "env_material_id","env_package_id","geo_loc_name_id", "dna_region_id",'adapter_sequence_id','sequencing_platform_id','target_gene_id','domain_id','illumina_index_id','primer_suite_id', 'run_id' ];

req_first_col = ['#SampleID','sample_name','dataset_name']
#test = ('434','0','y','1/27/14','0','GAZ:Canada','167.5926056','ENVO:urban biome','ENVO:human-associated habitat','ENVO:feces','43.119339','-79.2458198','y',
#'408170','human gut metagenome','American Gut Project Stool sample')
#test7 = ('434','ENVO:urban biome','ENVO:human-associated habitat','ENVO:feces','43.119339','-79.2458198','y')
id_queries = [
    {"table":"term","query": "SELECT term_id FROM term WHERE term_name = 'unknown'"},
    {"table":"env_package","query": "SELECT env_package_id FROM env_package WHERE env_package = 'unknown'"},
    {"table":"dna_region","query": "SELECT dna_region_id FROM dna_region WHERE dna_region = 'unknown'"},
    {"table":"adapter_sequence","query": "SELECT run_key_id FROM run_key WHERE run_key = 'unknown'"},   # adapter_sequence
    {"table":"sequencing_platform","query": "SELECT sequencing_platform_id FROM sequencing_platform WHERE sequencing_platform = 'unknown'"},
    {"table":"target_gene","query": "SELECT target_gene_id FROM target_gene WHERE target_gene = 'unknown'"},
    {"table":"domain","query": "SELECT domain_id FROM domain WHERE domain = 'unknown'"},
    {"table":"illumina_index","query": "SELECT illumina_index_id FROM illumina_index WHERE illumina_index = 'unknown'"},
    {"table":"primer_suite","query": "SELECT primer_suite_id FROM primer_suite WHERE primer_suite = 'unknown'"},
    {"table":"run","query": "SELECT run_id FROM run WHERE run = 'unknown'"}
]


def start_metadata_load(args):
    global mysql_conn, cur
    logging.info('CMD> '+' '.join(sys.argv))
    
    if args.host == 'vamps' or args.host == 'vampsdb':
        hostname = 'vampsdb'
    elif args.host == 'vampsdev':
        hostname = 'vampsdev'
    else:
        hostname = 'localhost'
        args.NODE_DATABASE = 'vamps_development'
    
    mysql_conn = MySQLdb.connect(db = args.NODE_DATABASE, host=hostname, read_default_file=os.path.expanduser("~/.my.cnf_node")  )
    cur = mysql_conn.cursor()
    dataset_ids = get_dataset_ids(args)
    # no metadata -- should enter defaults
    print ("Using 'unknown' Defaults")
    defaults = get_null_ids()
    # must get dids
    #print (DATASET_ID_BY_NAME.values())
    #print (defaults)
    #{'term': 6191, 'dna_region': 1, 'adapter_sequence': 1, 'sequencing_platform': 5, 'target_gene': 3, 'domain': 1, 'illumina_index': 83, 'primer_suite': 35, 'run': 5543}
    q = "INSERT IGNORE into required_metadata_info (dataset_id"
    for id_label in required_id_metadata_fields:
        q += ", "+ id_label+'_id'
    q+= ") VALUES"
    for did in dataset_ids:
        q +=" ("+ str(did)
        for id_label in required_id_metadata_fields:
            #print(id_label)
            if id_label == 'env_material' or id_label == 'env_feature' or id_label == 'env_biome' or id_label == 'geo_loc_name':
                entry = str(defaults['term'])
            else:
                entry = str(defaults[id_label])
            q += ","+entry
        
        q += "),"
    q = q[:-1]  # remove trailing comma
    print(dataset_ids)
    cur.execute(q)
    mysql_conn.commit()

def get_null_ids():
    global mysql_conn, cur
    unknowns = {}
        
    for q in id_queries:
        cur.execute(q['query'])
        #print(q['query'])
        mysql_conn.commit()
        row = cur.fetchone()
        unknowns[q['table']] = row[0]
    
    print ('unknown IDs',unknowns)
    return unknowns
    
def put_required_metadata():
    global mysql_conn, cur
    q = "INSERT IGNORE into required_metadata_info (dataset_id,"+','.join(required_metadata_fields)+")"
    q = q+" VALUES("
    
    for i,did in enumerate(REQ_METADATA_ITEMS['dataset_id']):
        vals = "'"+str(did)+"',"
        
        for item in required_metadata_fields:
            if item in REQ_METADATA_ITEMS:
                vals += "'"+str(REQ_METADATA_ITEMS[item][i])+"',"
            else:
                vals += "'',"
        q2 = q + vals[:-1] + ")"  
        print(q2)
        logging.info(q2)
        #cur.execute(q2)
    #mysql_conn.commit()
    
def get_dataset_ids(args):
    q = "SELECT dataset_id from dataset where project_id='%s'" % (args.pid)
    print(q)
    cur.execute(q)
    dataset_ids = []
    for row in cur.fetchall():
        did = str(row[0])
        dataset_ids.append(did)
    return dataset_ids
    
    
if __name__ == '__main__':
    import argparse
    
    
    myusage = """usage: assign_default_metadata.py  [options]
         
         Assisgns 'Unknown' to required metadata
         where
            -db NODE_DATABASE  (default: vamps2)
            -host required  vamps, vampsdev
            -pid   project_id to give default req metadata to         
            
    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)                 
    
       
    parser.add_argument('-db', '--NODE_DATABASE',         
                required=False,   action="store",  dest = "NODE_DATABASE",    default='vamps2',       
                help = 'node database')          
    parser.add_argument("-pid", "--pid",    
                required=True,  action="store",   dest = "pid", 
                help = '')     
    parser.add_argument("-host", "--host",    
                required=True,  action="store",   dest = "host", default='local',
                help = '')
    
    args = parser.parse_args()    
   
    args.datetime     = str(datetime.date.today())    
    
    start_metadata_load(args)
    
    
        
    
        
