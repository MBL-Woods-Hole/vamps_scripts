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
import configparser as ConfigParser
sys.path.append( '/Users/avoorhis/programming/vamps-node.js/public/scripts/maintenance_scripts' )

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
required_metadata_fields_with_ids = [  "collection_date","env_biome_id", "env_feature_id", "env_material_id", "env_package_id","geo_loc_name_id","latitude", "longitude", "dna_region_id",'adapter_sequence_id','sequencing_platform_id','target_gene_id','domain_id','illumina_index_id','primer_suite_id', 'run_id'];

required_id_metadata_fields= [  "env_biome", "env_feature", "env_material","env_package","geo_loc_name", "dna_region",'adapter_sequence','sequencing_platform','target_gene','domain','illumina_index','primer_suite', 'run' ];
required_id_metadata_fields2= [  "env_biome_id", "env_feature_id", "env_material_id","env_package_id","geo_loc_name_id", "dna_region_id",'adapter_sequence_id','sequencing_platform_id','target_gene_id','domain_id','illumina_index_id','primer_suite_id', 'run_id' ];

#required_id_metadata_fields= [  "env_biome_id", "env_feature_id", "env_material_id","env_package_id","geo_loc_name_id", "dna_region_id",'adapter_sequence_id','sequencing_platform_id','target_gene_id','domain_id','illumina_index_id','primer_suite_id', 'run_id' ];

req_first_col = ['#SampleID','sample_name','dataset_name','dataset','datasets','sample name']
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
    {"table":"illumina_index","query": "SELECT illumina_index_id FROM illumina_index WHERE illumina_index = 'none'"},
    {"table":"primer_suite","query": "SELECT primer_suite_id FROM primer_suite WHERE primer_suite = 'unknown'"},
    {"table":"run","query": "SELECT run_id FROM run WHERE run = 'unknown'"}
]


def start_metadata_load_from_file(args):
    global mysql_conn, cur
    logging.info('CMD> '+' '.join(sys.argv))
    
    if args.site == 'vamps' or args.site == 'vampsdb' or args.site == 'bpcweb8':
        hostname = 'vampsdb'
        args.NODE_DATABASE = 'vamps2'
    elif args.site == 'vampsdev' or args.site == 'bpcweb7':
        hostname = 'bpcweb7'
        args.NODE_DATABASE = 'vamps2'
    else:
        hostname = 'localhost'
        args.NODE_DATABASE = 'vamps_development'
    
    mysql_conn = MySQLdb.connect(db = args.NODE_DATABASE, host=hostname, read_default_file=os.path.expanduser("~/.my.cnf_node")  )
    cur = mysql_conn.cursor()
    
    csv_infile =   os.path.join(args.project_dir,args.metadata_file)
    get_config_data(args)  
    unknowns = get_null_ids()
    if os.path.isfile(csv_infile):
        cur.execute("USE "+args.NODE_DATABASE)    
          
        get_metadata(args.project_dir,csv_infile)
        print(REQ_METADATA_ITEMS.keys())
        print(CUST_METADATA_ITEMS.keys())
        
       
        if len(CUST_METADATA_ITEMS.keys()) > 2:
            put_custom_metadata()
        else:
            print('NO CUSTOM METADATA')
        
        if len(REQ_METADATA_ITEMS.keys()) > 2:
            put_required_metadata(unknowns)
        else:
            print('NO REQUIRED METADATA')
        
    else:
        print("Could not find csv_file:",csv_infile)
        # no metadata -- should enter defaults
        print("Using 'unknown' Defaults")
        # must get dids
        
        #{'term': 6191, 'dna_region': 1, 'adapter_sequence': 1, 'sequencing_platform': 5, 'target_gene': 3, 'domain': 1, 'illumina_index': 83, 'primer_suite': 35, 'run': 5543}
        q = "INSERT IGNORE into required_metadata_info (dataset_id"
        for id_label in required_id_metadata_fields:
            q += ", "+ id_label+'_id'
        q+= ") VALUES"
        for did in DATASET_ID_BY_NAME.values():
            q +=" ("+ str(did)
            for id_label in required_id_metadata_fields:
                #print(id_label)
                if id_label == 'env_material' or id_label == 'env_feature' or id_label == 'env_biome' or id_label == 'geo_loc_name':
                    entry = str(unknownss['term'])
                else:
                    entry = str(unknowns[id_label])
                q += ","+entry
            
            q += "),"
        q = q[:-1]  # remove trailing comma
        print(q)
        cur.execute(q)
    mysql_conn.commit()
    return CONFIG_ITEMS['project_id']

def get_null_ids():
    global mysql_conn, cur
    unknowns = {}
        
    for q in id_queries:
        cur.execute(q['query'])
        mysql_conn.commit()
        row = cur.fetchone()
        unknowns[q['table']] = row[0]
    
    print('unknown IDs',unknowns)
    return unknowns
    
def put_required_metadata(unknowns):
    global mysql_conn, cur
    print('Starting put_required_metadata')
    print('args.update')
    print(args.update)
    if args.update:
        # UPDATE
        for i,did in enumerate(REQ_METADATA_ITEMS['dataset_id']):
            q3 = "UPDATE required_metadata_info set "   #(dataset_id,"+','.join(required_metadata_fields)+")"
            for item in required_metadata_fields:
                if item+'_id' in required_id_metadata_fields2:
                    newitem=item+'_id'                    
                else:
                    newitem=item
                #if newitem == 'latitude' and (str(REQ_METADATA_ITEMS[newitem][i]) =='0' or str(REQ_METADATA_ITEMS[newitem][i])==''):
                if newitem in REQ_METADATA_ITEMS:
                    if newitem in ['latitude','longitude'] and (str(REQ_METADATA_ITEMS[newitem][i]) =='0' or str(REQ_METADATA_ITEMS[newitem][i])==''):
                        q3 += newitem+"=null,"
                    else:
                        q3 += newitem+"='"+str(REQ_METADATA_ITEMS[newitem][i])+"',"
                else:
                    print(newitem)
                    if newitem in ['env_biome_id','env_feature_id','env_material_id','geo_loc_name_id']:
                        if item in REQ_METADATA_ITEMS:
                            search = REQ_METADATA_ITEMS[item][i]
                        else:
                            search = 'unknown'
                        q2 = "select term_id from term where term_name='"+search+"'"
                    elif  newitem == 'adapter_sequence_id':
                        if item in REQ_METADATA_ITEMS:
                            search = REQ_METADATA_ITEMS[item][i]
                        else:
                            search = 'unknown'
                        q2 = "select run_key_id from run_key where run_key='"+search+"'" 
                    else:
                        if item in REQ_METADATA_ITEMS:
                            search = REQ_METADATA_ITEMS[item][i]
                        else:
                            search = 'unknown'
                        q2 = "select "+newitem+" from "+newitem[:-3]+" where "+newitem[:-3]+"='"+search+"'"
                    
                    print(q2)
                    cur.execute(q2)
                    if cur.rowcount > 0:
                        row = cur.fetchone()
                        print(row[0])
                        q3 += newitem+"='"+str(row[0])+"',"
                    else:
                        print('unknowns')
                        print(unknowns)
                        try:
                            q3 += newitem+"='"+str(unknowns[item])+"',"
                        except KeyError:
                            q3 += newitem+"='"+str(unknowns[newitem])+"'," 
                        except:
                            q3 += newitem+"='',"
                    
               
            q3 = q3[:-1] + " WHERE dataset_id='"+str(did)+"'"
            print(q3)
            
            cur.execute(q3)
            mysql_conn.commit()
            
    else:   # insert: this is default
        # INSERT
        q = "INSERT IGNORE into required_metadata_info (dataset_id,"+','.join(required_metadata_fields_with_ids)+")"
        q = q+" VALUES("
    
        for i,did in enumerate(REQ_METADATA_ITEMS['dataset_id']):
            vals = "'"+str(did)+"',"
            
            for item in required_metadata_fields:
                print('item: '+item)
                if item+'_id' in required_id_metadata_fields2:
                    newitem=item+'_id'                    
                else:
                    newitem=item
                # newitem is item with '_id' if needed                
                if newitem in REQ_METADATA_ITEMS:
                    vals += "'"+str(REQ_METADATA_ITEMS[newitem][i])+"',"
                else:
                    print('newitem: '+newitem)
                    
                    
                    if newitem in ['env_biome_id','env_feature_id','env_material_id','geo_loc_name_id']:
                        if item in REQ_METADATA_ITEMS:
                            search = REQ_METADATA_ITEMS[item][i]
                        else:
                            search = 'unknown'
                        q2 = "select term_id from term where term_name='"+search+"'"
                    elif  newitem == 'adapter_sequence_id':
                        if item in REQ_METADATA_ITEMS:
                            search = REQ_METADATA_ITEMS[item][i]
                        else:
                            search = 'unknown'
                        q2 = "select run_key_id from run_key where run_key='"+search+"'" 
                    else:
                        print(REQ_METADATA_ITEMS)
                        if item in REQ_METADATA_ITEMS:
                            search = REQ_METADATA_ITEMS[item][i]
                        else:
                            if newitem == 'illumina_index_id':
                                search = 'none'
                            else:
                                search = 'unknown'
                        q2 = "select "+newitem+" from "+newitem[:-3]+" where "+newitem[:-3]+"='"+search+"'"
                    
                
                    
                    print(q2)
                    cur.execute(q2)
                    if cur.rowcount > 0:
                        row = cur.fetchone()
                        print(row[0])
                        vals += "'"+str(row[0])+"',"
                    else:
                        print('unknowns')
                        print(unknowns)
                        try:
                            vals += "'"+str(unknowns[item])+"',"
                        except:
                            vals += "'"+str(unknowns[newitem])+"',"
                    
            q3 = q + vals[:-1] + ")"  
            print(q3)
            
            cur.execute(q3)
            mysql_conn.commit()
            
            
def put_custom_metadata():
    """
      create new table
    """
    global mysql_conn, cur
    print('starting put_custom_metadata')
    cust_keys_array = CUST_METADATA_ITEMS.keys()
    custom_table = 'custom_metadata_'+str(CONFIG_ITEMS['project_id'])
    # for insert[Default] and update delete then recreate custom table
    #if args.insert_or_update == 'update':
        # delete custom table 

    q = "DROP TABLE IF EXISTS `"+ custom_table +"`"
    cur.execute(q)
    print(q)
    mysql_conn.commit()
    # delete dataset_ids from 
    q = "DELETE from custom_metadata_fields WHERE project_id='"+str(CONFIG_ITEMS['project_id'])+"'"
    print(q)
    cur.execute(q)
    mysql_conn.commit()

    # TABLE-1 === custom_metadata_fields
    for key in CUST_METADATA_ITEMS:
        logging.debug(key)
        q2 = "INSERT IGNORE into custom_metadata_fields(project_id,field_name,example)"
        q2 += " VALUES("
        q2 += "'"+str(CONFIG_ITEMS['project_id'])+"',"
        q2 += "'"+key+"',"
        
        q2 += "'"+str(CUST_METADATA_ITEMS[key][0])+"')"
        logging.info(q2)
        cur.execute(q2)
    
    # TABLE-2 === custom_metadata_<pid>
    
    q = "CREATE TABLE IF NOT EXISTS `"+ custom_table + "` (\n"
    q += " `"+custom_table+"_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
    q += " `project_id` int(11) unsigned NOT NULL,\n"
    q += " `dataset_id` int(11) unsigned NOT NULL,\n"
    for key in cust_keys_array:
        if key != 'dataset_id':
            # 2015-07-23 Changed the field type to text to accomidate longer metadata
            # limit to 1000 chars below (line 143)
            #q += " `"+key+"` varchar(128) NOT NULL,\n" 
            q += " `"+key+"` text NOT NULL,\n"
    q += " PRIMARY KEY (`"+custom_table+"_id` ),\n" 
    unique_key = "UNIQUE KEY `unique_key` (`project_id`,`dataset_id`,"
    
    # ONLY 16 key items allowed:    
 #   for i,key in enumerate(cust_keys_array):
 #       if i < 14 and key != 'dataset_id':
  #          unique_key += " `"+key+"`,"
 #   q += unique_key[:-1]+"),\n"
    q += " KEY `project_id` (`project_id`),\n"
    q += " KEY `dataset_id` (`dataset_id`),\n"
    q += " CONSTRAINT `"+custom_table+"_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `project` (`project_id`) ON UPDATE CASCADE,\n"
    q += " CONSTRAINT `"+custom_table+"_ibfk_2` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`dataset_id`) ON UPDATE CASCADE\n"
    q += " ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
    logging.info(q)
    cur.execute(q)
    
    for i,did in enumerate(CUST_METADATA_ITEMS['dataset_id']):
    
        q2 = "INSERT IGNORE into "+custom_table+" (project_id,dataset_id,"
        for key in cust_keys_array:
            if key != 'dataset_id':
                q2 += "`"+key+"`,"
        q2 = q2[:-1]+ ")"
        q2 += " VALUES('"+str(CONFIG_ITEMS['project_id'])+"','"+str(did)+"',"
        for key in cust_keys_array:
            if key != 'dataset_id':


                val = str(CUST_METADATA_ITEMS[key][i])[:1000]   # limit at 1000 chars
                q2 += "'"+val+"',"
        q2 = q2[:-1] + ")"    # remove trailing comma

        print(q2)
        logging.info(q2)
        cur.execute(q2)
    
    mysql_conn.commit()
    
def get_metadata(indir, csv_infile):
    
    
    print('csv',csv_infile)
    logging.info('csv '+csv_infile)
    if args.delim == 'tab':
        lol = list(csv.reader(open(csv_infile, 'r'), delimiter='\t'))
    elif args.delim == 'comma':
        lol = list(csv.reader(open(csv_infile, 'r'), delimiter=','))
    else:
        sys.exit('Could not find delimiter: `tab` or `comma`')
    TMP_METADATA_ITEMS = {}
    
    
    keys = [k.lower() for k in lol[0]]
    print('keys')
    print(keys)
    for i,key in enumerate(keys):
        TMP_METADATA_ITEMS[key] = []
        for line in lol[1:]:
            if line:
                TMP_METADATA_ITEMS[key].append(line[i])
    saved_indexes = []    
 
   #if CONFIG_ITEMS['fasta_type']=='multi':           
    for ds in CONFIG_ITEMS['datasets']:
        
        found = False
        for samp_head_name in req_first_col:
            #print('samp_head_name',samp_head_name)
            if samp_head_name in TMP_METADATA_ITEMS:
                found = True                
                #print('found: '+samp_head_name)
                try:
                    saved_indexes.append(TMP_METADATA_ITEMS[samp_head_name].index(ds))
                    dataset_header_name = samp_head_name
                    print('USING: "'+dataset_header_name+'"')
                except:
                    pass
            
        if not found:
            print(req_first_col)
            sys.exit('ERROR: Could not find "dataset" column in file')
                                
    
    # now get the data from just the datasets we have in CONFIG.ini
   
    for key in TMP_METADATA_ITEMS:
        
        if key in required_metadata_fields:
            REQ_METADATA_ITEMS[key] = []
            REQ_METADATA_ITEMS['dataset_id'] = []
            for j,value in enumerate(TMP_METADATA_ITEMS[key]):
                
                #if CONFIG_ITEMS['fasta_type']=='multi' and j in saved_indexes:
                if j in saved_indexes:
                    if key in required_metadata_fields:
                        REQ_METADATA_ITEMS[key].append(TMP_METADATA_ITEMS[key][j])
                    
                    #if CONFIG_ITEMS['fasta_type']=='multi':
                    ds = TMP_METADATA_ITEMS[dataset_header_name][j]
                    #else:
                    #    ds = CONFIG_ITEMS['datasets'][0]
                    did = DATASET_ID_BY_NAME[ds]
                    REQ_METADATA_ITEMS['dataset_id'].append(did)
                else:
                    if key in required_metadata_fields:
                        REQ_METADATA_ITEMS[key].append(TMP_METADATA_ITEMS[key][j])
        else:
            CUST_METADATA_ITEMS[key] = []
            CUST_METADATA_ITEMS['dataset_id'] = []
            
            for j,value in enumerate(TMP_METADATA_ITEMS[key]):
                
                #if CONFIG_ITEMS['fasta_type']=='multi' and j in saved_indexes:
                if j in saved_indexes:   
                    if key not in required_metadata_fields:                        
                        CUST_METADATA_ITEMS[key].append(TMP_METADATA_ITEMS[key][j].replace('"','').replace("'",''))
                    #if CONFIG_ITEMS['fasta_type']=='multi':
                    ds = TMP_METADATA_ITEMS[dataset_header_name][j]
                    #else:
                    #    ds = CONFIG_ITEMS['datasets'][0]
                    did = DATASET_ID_BY_NAME[ds]
                    CUST_METADATA_ITEMS['dataset_id'].append(did)
                else:
                    if key not in required_metadata_fields:                        
                        CUST_METADATA_ITEMS[key].append(TMP_METADATA_ITEMS[key][j].replace('"','').replace("'",''))
                    ds = CONFIG_ITEMS['datasets'][0]
                    did = DATASET_ID_BY_NAME[ds]
                    CUST_METADATA_ITEMS['dataset_id'].append(did)
    
               
    if not 'dataset_id' in REQ_METADATA_ITEMS:
        REQ_METADATA_ITEMS['dataset_id'] = []
    if 'dataset_id' not in CUST_METADATA_ITEMS:
        CUST_METADATA_ITEMS['dataset_id'] = []
            
def get_config_data(args):
    global mysql_conn, cur
    config_path = os.path.join(args.project_dir, args.config_file)
    print(config_path)
    logging.info(config_path)
    config = ConfigParser.ConfigParser()
    config.optionxform=str
    config.read(config_path)    
    for name, value in  config.items('MAIN'):
        
        CONFIG_ITEMS[name] = value
    CONFIG_ITEMS['datasets'] = []
    for dsname, count in  config.items('MAIN.dataset'):        
        CONFIG_ITEMS['datasets'].append(dsname)   
    
    q = "SELECT project_id FROM project"
    q += " WHERE project = '"+CONFIG_ITEMS['project_name']+"'" 
    logging.info(q)
    print(q)
    cur.execute(q)
    
    if cur.rowcount != 1:
        sys.exit("Project query error- Exiting!")
    row = cur.fetchone()     
    CONFIG_ITEMS['project_id'] = row[0]
        
    q = "SELECT dataset,dataset_id from dataset"
    q += " WHERE dataset in('"+"','".join(CONFIG_ITEMS['datasets'])+"')"
    logging.info(q)
    cur.execute(q)     
    for row in cur.fetchall():        
        DATASET_ID_BY_NAME[row[0]] = row[1]
        
    mysql_conn.commit()
    

    

    
if __name__ == '__main__':
    import argparse
    
    
    myusage = """usage: vamps_script_upload_metadata.py  [options]
         
         
         where
            -db/--NODE_DATABASE         DEFAULT: vamps_development (unless -site == vamps or vampsdev)
            -project_dir/--project_dir   Required
            -p/--project
            -site/--site                DEFAULT: local
            -config/--config            name only
            -in/--infile                name only [DEFAULT: `metadata_clean.csv`]
            -u/--update                 Update will overwrite! [DEFAULT: insert/ no overwrite]   
    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)                 
    
       
    parser.add_argument('-db', '--NODE_DATABASE',         
                required=False,   action="store",  dest = "NODE_DATABASE",    default='vamps_development',       
                help = 'node database')  
    parser.add_argument("-project_dir", "--project_dir",    
                required=True,  action="store",   dest = "project_dir", 
                help = '')          
    parser.add_argument("-p", "--project",    
                required=True,  action="store",   dest = "project", 
                help = '')     
    parser.add_argument("-site", "--site",    
                required=False,  action="store",   dest = "site", default='local',
                help = '')
    parser.add_argument("-config", "--config",    
                required=True,  action="store",   dest = "config_file", 
                help = 'config file name') 
    parser.add_argument("-in", "--infile",    
                required=False,  action="store",   dest = "metadata_file", default='metadata_clean.csv',
                help = 'config file name') 
    parser.add_argument("-ds", "--dataset_name_header",
                required=False,  action="store",   dest = "dataset_name_header", default='dataset',
                help = 'column to use for dataset name')
    parser.add_argument("-delim","--delimiter",
                required=False,  action="store",   dest = "delim", default='comma',
                help="""METADATA: comma or tab""")
    
    parser.add_argument("-u","--update",
                required=False,  action="store_true",   dest = "update", default=False,
                help="""insert or update""")
                
    args = parser.parse_args()    
   
    args.datetime     = str(datetime.date.today())    
    
    
    pid = start_metadata_load_from_file(args)
    print('Finished PID:'+str(pid))
    #sys.exit('END: vamps_script_upload_metadata.py')
    
        
    
        
