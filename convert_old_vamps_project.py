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
import ConfigParser
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
#NODE_DATABASE = "vamps_js_dev_av"
#NODE_DATABASE = "vamps_js_development"
CONFIG_ITEMS = {}
SEQ_COLLECTOR = {}
DATASET_ID_BY_NAME = {}
SILVA_IDS_BY_TAX = {}
RANK_COLLECTOR={}
TAX_ID_BY_RANKID_N_TAX = {}
SUMMED_TAX_COLLECTOR = {}  # SUMMED_TAX_COLLECTOR[ds][rank][tax_string] = count
ranks =['domain','phylum','klass','order','family','genus','species','strain']
REQ_METADATA_ITEMS = {}
CUST_METADATA_ITEMS = {}

required_metadata_fields = [ "altitude", "assigned_from_geo", "collection_date", "depth", "country", "elevation", "env_biome", "env_feature", "env_matter", "latitude", "longitude", "public","taxon_id","description","common_name"];
classifiers = {"GAST":{'ITS1':1,'SILVA108_FULL_LENGTH':2,'GG_FEB2011':3,'GG_MAY2013':4},
                "RDP":{'ITS1':6,'2.10.1':5,'GG_FEB2011':7,'GG_MAY2013':8},
                'unknown':{'unknown':9}}
# ranks =[{'name':'domain', 'id':1,'num':0},
#         {'name':'phylum', 'id':4,'num':1},
#         {'name':'klass',  'id':5,'num':2},
#         {'name':'order',  'id':6,'num':3},
#         {'name':'family', 'id':8,'num':4},
#         {'name':'genus',  'id':9,'num':5},
#         {'name':'species','id':10,'num':6},
#         {'name':'strain', 'id':11,'num':7}]

LOG_FILENAME = os.path.join('.','convert_old_vamps_project.log')
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILENAME, filemode="w",
                           format="%(asctime)-15s %(levelname)-8s %(message)s")
#logging = logging.getlogging('')
#os.chdir(args.indir)

def start(args):

    global mysql_conn
    global cur
    seqs_file_lines = list(csv.reader(open(args.seqs_file, 'rb'), delimiter=','))[1:]

    logging.debug('starting convert_old_vamps_project.log')

    
    my_class           = Old_vamps_data(args)
    DATASET_ID_BY_NAME = my_class.dataset_id_by_name_dict
    PROJECT_ID_BY_NAME = my_class.project_id_by_name_dict

    logging.debug("checking user")
    check_user(args,my_class.cursor)  ## script dies if user not in db
    logging.debug("checking project")
    # uncomment
    check_project(args,my_class.cursor)

    logging.debug("running get_config_data")
    logging.debug("running get_config_data")
    get_config_data(args)

    print CONFIG_ITEMS
    #print DATASET_ID_BY_NAME
    #print PROJECT_ID_BY_NAME
    
    #
    logging.debug("recreating ranks")
    # uncomment
    recreate_ranks(my_class)
    #
    logging.debug("env sources")
    # uncomment
    #    create_env_source()
    #
    logging.debug("classifier")
    # uncomment
    #    create_classifier()
    #
    logging.debug("starting taxonomy")
    # uncomment
    push_taxonomy(args,my_class)
    #
    sys.exit()
    logging.debug("starting sequences")
    # uncomment
    push_sequences(args,my_class)

    logging.debug("projects")
    push_project(PROJECT_ID_BY_NAME)

    logging.debug("datasets")
    push_dataset()

    #push_summed_counts()
    logging.debug("starting push_pdr_seqs")
    # uncomment
    #    push_pdr_seqs()

    my_class.collect_datasets(seqs_file_lines)
    
    # print "CUST_METADATA_ITEMS before start_metadata = "
#     print CUST_METADATA_ITEMS
#     
#     logging.debug("starting metadata")
#     start_metadata(args, DATASET_ID_BY_NAME, my_class)

    #print SEQ_COLLECTOR
    #pp.pprint(CONFIG_ITEMS)

    return CONFIG_ITEMS['project_id']


def check_user(args,cur):
    """
    check_user()
    the owner/user (from config file) must be present in 'user' table for script to continue
    """
    q = "select user_id from user where username='"+args.owner+"'"
    cur.execute(q)
    numrows = int(cur.rowcount)
    if numrows==0:
        sys.exit('Could not find owner: '+args.owner+' --Exiting')
    else:
        row = cur.fetchone()
        CONFIG_ITEMS['owner_id'] = row[0]

def check_project(args,cur):
    """
    check_project()
    the project must not already exist in db table 'project' for script to continue
    """
    q = "select project_id from project where project='"+args.project+"'"
    cur.execute(q)
    numrows = int(cur.rowcount)
    if numrows > 0:
        sys.exit('Project already Exists: '+args.project+' --Exiting')

def create_env_source():
    q = "INSERT IGNORE INTO env_sample_source VALUES (0,''),(10,'air'),(20,'extreme habitat'),(30,'host associated'),(40,'human associated'),(45,'human-amniotic-fluid'),(47,'human-blood'),(43,'human-gut'),(42,'human-oral'),(41,'human-skin'),(46,'human-urine'),(44,'human-vaginal'),(140,'indoor'),(50,'microbial mat/biofilm'),(60,'miscellaneous_natural_or_artificial_environment'),(70,'plant associated'),(80,'sediment'),(90,'soil/sand'),(100,'unknown'),(110,'wastewater/sludge'),(120,'water-freshwater'),(130,'water-marine')"
    cur.execute(q)
    mysql_conn.commit()

def create_classifier():
    q = "INSERT IGNORE INTO classifier VALUES" # (1,'GAST','ITS1'),(2,'GAST','SILVA108_FULL_LENGTH'),(3,'GAST','GG_FEB2011'),(4,'GAST','GG_MAY2013'),"
    for classifier in classifiers:
        for db in classifiers[classifier]:
            id = str(classifiers[classifier][db])
            q += "('"+id+"','"+classifier+"','"+db+"'),"
    q = q[:-1]
    cur.execute(q)
    mysql_conn.commit()

def recreate_ranks(my_class):
    cur = my_class.cursor
    for i,rank in enumerate(ranks):

        q = "INSERT IGNORE into rank (rank,rank_number) VALUES('"+rank+"','"+str(i)+"')"
        logging.debug(q)
        cur.execute(q)
        rank_id = cur.lastrowid
        if rank_id==0:
            q = "SELECT rank_id from rank where rank='"+rank+"'"
            logging.debug(q)
            cur.execute(q)
            row = cur.fetchone()
            RANK_COLLECTOR[rank] = row[0]
        else:
            RANK_COLLECTOR[rank] = rank_id
    q = "INSERT IGNORE into rank (rank,rank_number) VALUES('superkingdom','0'),('NA','0')"
    cur.execute(q)
    my_class.mysql_util.conn.commit()

def push_dataset():
    print "IN push_dataset CONFIG_ITEMS"
    print CONFIG_ITEMS
    fields = ['dataset','dataset_description','env_sample_source_id','project_id']
    q = "INSERT ignore into dataset ("+(',').join(fields)+")"
    q += " VALUES('%s','%s','%s','%s')"
    for ds in CONFIG_ITEMS['datasets']:
        desc = ds+'_description'
        #print ds,desc,CONFIG_ITEMS['env_source_id'],CONFIG_ITEMS['project_id']
        q4 = q % (ds, desc, CONFIG_ITEMS['env_source_id'], CONFIG_ITEMS['project_id'])
        logging.debug(q4)
        try:
            cur.execute(q4)
            did = cur.lastrowid
            DATASET_ID_BY_NAME[ds] = str(did)
        except:
            logging.debug('ERROR: MySQL Integrity ERROR -- duplicate dataset')
            sys.exit('ERROR: MySQL Integrity ERROR -- duplicate dataset')
    print "DATASET_ID_BY_NAME"
    print DATASET_ID_BY_NAME
    mysql_conn.commit()

def push_project(PROJECT_ID_BY_NAME):
    try: 
      CONFIG_ITEMS['project_id'] = PROJECT_ID_BY_NAME[args.project]
    except KeyError:
      insert_project(CONFIG_ITEMS, args)
    except:
      raise
      
    # if args.add_project:
   #      # q = "SELECT project_id from project where project='%s'" % (args.project)
   #      # logging.debug(q)
   #      # cur.execute(q)
   #      # mysql_conn.commit()
   #      # row = cur.fetchone()
   #      # CONFIG_ITEMS['project_id'] = row[0]
   #
   #      print("ADD TO PID="+str(CONFIG_ITEMS['project_id']))
   #      logging.debug("ADDING to project -- PID="+str(CONFIG_ITEMS['project_id']))
   #  else:
   #      # uncomment
   #      # q = "INSERT into project ("+(',').join(fields)+")"
   #      q = "INSERT IGNORE into project ("+(',').join(fields)+")"
   #      q += " VALUES('%s','%s','%s','%s','%s','%s','%s')"
   #      q = q % (proj,title,desc,rev,fund,id,pub)
   #      print(q)
   #      logging.debug(q)
   #      cur.execute(q)
   #      mysql_conn.commit()
   #      last_insert_id = cur.lastrowid
   #      if last_insert_id > 0:
   #        CONFIG_ITEMS['project_id'] = cur.lastrowid
   #      print("NEW PID="+str(CONFIG_ITEMS['project_id']))
   #      logging.debug("STARTING NEW project -- PID="+str(CONFIG_ITEMS['project_id']))


def insert_project(CONFIG_ITEMS, args):
    desc = "Project Description"
    title = "Title"
    proj = CONFIG_ITEMS['project']
    rev = CONFIG_ITEMS['project'][::-1]
    fund = "myfunding"
    id = CONFIG_ITEMS['owner_id']
    pub = CONFIG_ITEMS['public']
    fields = ['project','title','project_description','rev_project_name','funding','owner_user_id','public']
  
    if args.add_project:
        # q = "SELECT project_id from project where project='%s'" % (args.project)
        # logging.debug(q)
        # cur.execute(q)
        # mysql_conn.commit()
        # row = cur.fetchone()
        # CONFIG_ITEMS['project_id'] = row[0]
          
        print("ADD TO PID="+str(CONFIG_ITEMS['project_id']))
        logging.debug("ADDING to project -- PID="+str(CONFIG_ITEMS['project_id']))
    else:
        # uncomment
        # q = "INSERT into project ("+(',').join(fields)+")"
        q = "INSERT IGNORE into project ("+(',').join(fields)+")"
        q += " VALUES('%s','%s','%s','%s','%s','%s','%s')"
        q = q % (proj,title,desc,rev,fund,id,pub)
        print(q)
        logging.debug(q)
        cur.execute(q)
        mysql_conn.commit()
        last_insert_id = cur.lastrowid
        if last_insert_id > 0:
          CONFIG_ITEMS['project_id'] = cur.lastrowid
        print("NEW PID="+str(CONFIG_ITEMS['project_id']))
        logging.debug("STARTING NEW project -- PID="+str(CONFIG_ITEMS['project_id']))

def push_pdr_seqs():
    for ds in SEQ_COLLECTOR:
        for seq in SEQ_COLLECTOR[ds]:
            did = DATASET_ID_BY_NAME[ds]
            seqid = SEQ_COLLECTOR[ds][seq]['sequence_id']
            count = SEQ_COLLECTOR[ds][seq]['seq_count']
            q = "INSERT into sequence_pdr_info (dataset_id, sequence_id, seq_count,classifier_id)"
            q += " VALUES ('"+str(did)+"','"+str(seqid)+"','"+str(count)+"','2')"
            logging.debug(q)
            cur.execute(q)
    mysql_conn.commit()

def push_sequences(args,my_class):
    # sequences
    cur = my_class.cursor
    for ds in SEQ_COLLECTOR:
        for seq in SEQ_COLLECTOR[ds]:
            q = "INSERT ignore into sequence (sequence_comp) VALUES (COMPRESS('"+seq+"'))"
            logging.debug(q)
            cur.execute(q)
            my_class.mysql_util.conn.commit()
            seqid = cur.lastrowid
            if seqid == 0:
                q2 = "select sequence_id from sequence where sequence_comp = COMPRESS('"+seq+"')"
                logging.debug('DUP SEQ FOUND')
                cur.execute(q2)
                mysql_conn.commit()
                row = cur.fetchone()
                seqid=row[0]
            SEQ_COLLECTOR[ds][seq]['sequence_id'] = seqid
            silva_tax_id = str(SEQ_COLLECTOR[ds][seq]['silva_tax_id'])
            distance = str(SEQ_COLLECTOR[ds][seq]['distance'])
            logging.debug( ds+' - '+seq+' - '+str(silva_tax_id))
            rank_id = str(SEQ_COLLECTOR[ds][seq]['rank_id'])
            logging.debug( rank_id)
            q = "INSERT ignore into silva_taxonomy_info_per_seq"
            q += " (sequence_id,silva_taxonomy_id,gast_distance,refssu_id,rank_id)"
            q += " VALUES ('"+str(seqid)+"','"+silva_tax_id+"','"+distance+"','0','"+rank_id+"')"
            logging.debug(q)
            cur.execute(q)
            my_class.mysql_util.conn.commit()
            silva_tax_seq_id = cur.lastrowid
            logging.debug('1: '+str(silva_tax_seq_id))
            if silva_tax_seq_id == 0:
                q3 = "select silva_taxonomy_info_per_seq_id from silva_taxonomy_info_per_seq"
                q3 += " where sequence_id = '"+str(seqid)+"'"
                q3 += " and silva_taxonomy_id = '"+silva_tax_id+"'"
                q3 += " and gast_distance = '"+distance+"'"
                q3 += " and refssu_id = '0'"
                q3 += " and rank_id = '"+rank_id+"'"
                logging.debug('DUP silva_tax_seq')
                logging.debug(q3)
                cur.execute(q3)
                my_class.mysql_util.conn.commit()
                row = cur.fetchone()
                silva_tax_seq_id=row[0]
                logging.debug('0: '+str(silva_tax_seq_id))

            q4 = "INSERT ignore into sequence_uniq_info (sequence_id, silva_taxonomy_info_per_seq_id)"
            q4 += " VALUES('"+str(seqid)+"','"+str(silva_tax_seq_id)+"')"
            logging.debug(q4)
            cur.execute(q4)
            my_class.mysql_util.conn.commit()
        ## don't see that we need to save uniq_ids
    my_class.mysql_util.conn.commit()
    #print SEQ_COLLECTOR

def push_taxonomy(args,my_class):

    #print  general_config_items
    silva = ['domain_id','phylum_id','klass_id','order_id','family_id','genus_id','species_id','strain_id']
    accepted_domains = ['bacteria','archaea','eukarya','fungi','organelle','unknown']
    tax_collector = {}
    cur = my_class.cursor
    

    logging.debug( 'csv '+args.seqs_file)
    lines = list(csv.reader(open(args.seqs_file, 'rb'), delimiter=','))
    #print tax_file

    for line in lines:

        if line[0]=='id':
            continue
        logging.debug( line)
        seq = line[1]
        pj_file = line[2]
        ds = line[3]
        tax_string = line[4]
        refhvr_ids=line[5]
        rank = line[6]
        seq_count = line[7]
        distance = line[9]

        if pj_file != args.project:
            pass
            #sys.exit('Project file--name mismatch ('+pj_file+' - '+args.project+') -- Confused! Exiting!')

        if rank == 'class': rank = 'klass'
        if rank == 'orderx': rank = 'order'
        if ds not in CONFIG_ITEMS['datasets']:
            CONFIG_ITEMS['datasets'].append(ds)
        if ds not in SEQ_COLLECTOR:
            SEQ_COLLECTOR[ds]={}


        if ds not in SUMMED_TAX_COLLECTOR:
            SUMMED_TAX_COLLECTOR[ds]={}



        SEQ_COLLECTOR[ds][seq] = {'dataset':ds,
                              'taxonomy':tax_string,
                              'refhvr_ids':refhvr_ids,
                              'rank':rank,
                              'seq_count':seq_count,
                              'distance':distance
                              }
        q1 = "SELECT rank_id from rank where rank = '"+rank+"'"
        logging.debug( q1)
        cur.execute(q1)
        my_class.mysql_util.conn.commit()

        row = cur.fetchone()

        SEQ_COLLECTOR[ds][seq]['rank_id'] = row[0]

        tax_items = tax_string.split(';')
        #print tax_string
        sumtax = ''
        for i in range(0,8):

            rank_id = RANK_COLLECTOR[ranks[i]]
            if len(tax_items) > i:

                taxitem = tax_items[i]

            else:
                taxitem = ranks[i]+'_NA'
            sumtax += taxitem+';'

            #print ranks[i],rank_id,taxitem,sumtax,seq_count
            if rank_id in SUMMED_TAX_COLLECTOR[ds]:
                if sumtax[:-1] in SUMMED_TAX_COLLECTOR[ds][rank_id]:
                    SUMMED_TAX_COLLECTOR[ds][rank_id][sumtax[:-1]] += int(seq_count)
                else:
                    SUMMED_TAX_COLLECTOR[ds][rank_id][sumtax[:-1]] = int(seq_count)

            else:
                SUMMED_TAX_COLLECTOR[ds][rank_id] = {}
                SUMMED_TAX_COLLECTOR[ds][rank_id][sumtax[:-1]] = int(seq_count)

        #for i in range(0,8):
        #insert_nas()

        if tax_items[0].lower() in accepted_domains:
            ids_by_rank = []
            for i in range(0,8):
                #print i,len(tax_items),tax_items[i]
                rank_name = ranks[i]
                rank_id = RANK_COLLECTOR[ranks[i]]

                if len(tax_items) > i:
                    if ranks[i] == 'species':
                        t = tax_items[i].lower()
                    else:
                        t = tax_items[i].capitalize()

                    if tax_items[i].lower() != (rank_name+'_NA').lower():
                        name_found = False
                        if rank_name in tax_collector:
                            tax_collector[rank_name].append(t)
                        else:
                            tax_collector[rank_name] = [t]
                else:
                    t = rank_name+'_NA'



                q2 = "INSERT ignore into `"+rank_name+"` (`"+rank_name+"`) VALUES('"+t+"')"
                logging.debug(q2)
                cur.execute(q2)
                my_class.mysql_util.conn.commit()
                tax_id = cur.lastrowid
                if tax_id == 0:
                    q3 = "select "+rank_name+"_id from `"+rank_name+"` where `"+rank_name+"` = '"+t+"'"
                    logging.debug( q3 )
                    cur.execute(q3)
                    my_class.mysql_util.conn.commit()
                    row = cur.fetchone()
                    tax_id=row[0]
                ids_by_rank.append(str(tax_id))
                #else:
                logging.debug( 'rank_id,t,tax_id '+str(rank_id)+' - '+t+' - '+str(tax_id)  )
                if rank_id in TAX_ID_BY_RANKID_N_TAX:
                    TAX_ID_BY_RANKID_N_TAX[rank_id][t] = tax_id
                else:
                    TAX_ID_BY_RANKID_N_TAX[rank_id]={}
                    TAX_ID_BY_RANKID_N_TAX[rank_id][t] = tax_id
                #ids_by_rank.append('1')
            logging.debug(  ids_by_rank )
            q4 =  "INSERT ignore into silva_taxonomy ("+','.join(silva)+",created_at)"
            q4 += " VALUES("+','.join(ids_by_rank)+",CURRENT_TIMESTAMP())"
            #
            logging.debug(q4)
            cur.execute(q4)
            my_class.mysql_util.conn.commit()
            silva_tax_id = cur.lastrowid
            if silva_tax_id == 0:
                q5 = "SELECT silva_taxonomy_id from silva_taxonomy where ("
                vals = ''
                for i in range(0,len(silva)):
                    vals += ' '+silva[i]+"="+ids_by_rank[i]+' and'
                q5 = q5 + vals[0:-3] + ')'
                logging.debug(q5)
                cur.execute(q5)
                my_class.mysql_util.conn.commit()
                row = cur.fetchone()
                silva_tax_id=row[0]

            SILVA_IDS_BY_TAX[tax_string] = silva_tax_id
            SEQ_COLLECTOR[ds][seq]['silva_tax_id'] = silva_tax_id
            my_class.mysql_util.conn.commit()

    logging.debug( 'SUMMED_TAX_COLLECTOR')
    logging.debug( SUMMED_TAX_COLLECTOR)

def get_config_data(args):
    CONFIG_ITEMS['env_source_id'] = args.env_source_id
    CONFIG_ITEMS['public']= args.public
    CONFIG_ITEMS['owner'] = args.owner
    CONFIG_ITEMS['project'] = args.project
    CONFIG_ITEMS['datasets'] = []


def start_metadata(args, DATASET_ID_BY_NAME, my_class):

    #get_config_data(indir)
    get_metadata(args, DATASET_ID_BY_NAME)
    # uncomment
    #    put_required_metadata()
    # uncomment
    # put_custom_metadata()
    print "CUST_METADATA_ITEMS in start_metadata = "
    print CUST_METADATA_ITEMS
    
    my_class.put_custom_metadata_a(CUST_METADATA_ITEMS)
    # print "CONFIG_ITEMS"
    # print CONFIG_ITEMS
    logging.debug('REQ_METADATA_ITEMS '+str(REQ_METADATA_ITEMS))

    logging.debug('CUST_METADATA_ITEMS '+str(CUST_METADATA_ITEMS))

def put_required_metadata():



    #q_req = "INSERT into required_metadata_info (dataset_id,"+','.join(required_metadata_fields)+")"
    #q_req = q_req+" VALUES('"

    #for i,did in enumerate(REQ_METADATA_ITEMS['dataset_id']):
    for ds in CONFIG_ITEMS['datasets']:
        did = DATASET_ID_BY_NAME[ds]
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
        try:
            cur.execute(q2_req)

        except MySQLdb.Error, e:
            try:
                logging.debug("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            except IndexError:
                logging.debug("MySQL Error: %s" % str(e))



    mysql_conn.commit()

# def put_custom_metadata():
#     """
#       create new table
#     """
#
#     print "put_custom_metadata: CUST_METADATA_ITEMS"
#     print CUST_METADATA_ITEMS
#
#     logging.debug( 'starting put_custom_metadata')
#     # TABLE-1 === custom_metadata_fields
#     cust_keys_array = {}
#     all_cust_keys = []  # to create new table
#     print "CONFIG_ITEMS"
#     print CONFIG_ITEMS
#     for ds in CONFIG_ITEMS['datasets']:
#         did = DATASET_ID_BY_NAME[ds]
#         cust_keys_array[did]=[]
#         print "CUST_METADATA_ITEMS"
#         print CUST_METADATA_ITEMS
#
#         if did in CUST_METADATA_ITEMS:
#             print "did in CUST_METADATA_ITEMS"
#             print did
#             for key in CUST_METADATA_ITEMS[did]:
#                 if key not in all_cust_keys:
#                     all_cust_keys.append(key)
#                 if key not in cust_keys_array[did]:
#                     cust_keys_array[did].append(key)
#                 # q2 = "INSERT IGNORE into custom_metadata_fields(project_id, field_name, field_type, example)"
#                 q2 = "INSERT IGNORE into custom_metadata_fields(project_id, field_name, field_type, example)"
#                 q2 += " VALUES("
#                 q2 += "'"+str(CONFIG_ITEMS['project_id'])+"',"
#                 q2 += "'"+str(key)+"',"
#                 q2 += "'varchar(128)'," #? are they alvays the same? couldn't they by numbers?
#                 q2 += "'"+str(CUST_METADATA_ITEMS[did][key])+"')"
#                 print "put_custom_metadata: q2"
#                 print q2
#
#                 logging.debug(q2)
#                 cur.execute(q2)
#         mysql_conn.commit()
#
#
#     # TABLE-2 === CREATE custom_metadata_<pid>
#     custom_table = 'custom_metadata_'+str(CONFIG_ITEMS['project_id'])
#     q = "CREATE TABLE IF NOT EXISTS `"+ custom_table + "` (\n"
#     q += " `"+custom_table+"_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
#     # q += " `project_id` int(11) unsigned NOT NULL,\n"
#     q += " `dataset_id` int(11) unsigned NOT NULL,\n"
#     for key in all_cust_keys:
#         if key != 'dataset_id':
#             q += " `"+key+"` varchar(128) DEFAULT NULL,\n"
#     q += " PRIMARY KEY (`"+custom_table+"_id` ),\n"
#     # unique_key = "UNIQUE KEY `unique_key` (`project_id`,`dataset_id`,"
#     unique_key = "UNIQUE KEY `unique_key` (`dataset_id`,"
#
#     # ONLY 16 key items allowed:
#     for i,key in enumerate(all_cust_keys):
#         if i < 14 and key != 'dataset_id':
#             unique_key += " `"+key+"`,"
#     q += unique_key[:-1]+"),\n"
#     # q += " KEY `project_id` (`project_id`),\n"
#     q += " KEY `dataset_id` (`dataset_id`),\n"
#     # q += " CONSTRAINT `"+custom_table+"_ibfk_1` FOREIGN KEY (`project_id`) REFERENCES `project` (`project_id`) ON UPDATE CASCADE,\n"
#     q += " CONSTRAINT `"+custom_table+"_ibfk_2` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`dataset_id`) ON UPDATE CASCADE\n"
#     q += " ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
#     logging.debug(q)
#     cur.execute(q)
#
#
#     # add data
#     for ds in CONFIG_ITEMS['datasets']:
#         did = DATASET_ID_BY_NAME[ds]
#         # q3 = "INSERT into "+custom_table+" (project_id,dataset_id,"
#         q3 = "INSERT into "+custom_table+" (dataset_id"
#         for key in cust_keys_array[did]:
#             logging.debug("key in cust_keys_array[did] = ")
#             logging.debug(key)
#             if key != 'dataset_id':
#                 q3 += "`"+key+"`,"
#         q3 = q3[:-1]+ ")"
#         # q3 += " VALUES('"+str(CONFIG_ITEMS['project_id'])+"','"+str(did)+"',"
#         q3 += " VALUES('"+str(did)+"',"
#         for key in cust_keys_array[did]:
#             if key != 'dataset_id':
#                 if key in CUST_METADATA_ITEMS[did]:
#                     q3 += "'"+str(CUST_METADATA_ITEMS[did][key])+"',"
#         q3 = q3[:-1] + ")"
#         logging.debug(q3)
#         cur.execute(q3)
#
#     mysql_conn.commit()
#

def get_metadata(args, DATASET_ID_BY_NAME):
    logging.debug('csv '+str(args.metadata_file))
    if args.delim == 'comma':
        lines = list(csv.reader(open(args.metadata_file, 'rb'), delimiter=','))
    else:
        lines = list(csv.reader(open(args.metadata_file, 'rb'), delimiter='\t'))

    TMP_METADATA_ITEMS = {}
    for line in lines:
        #print line
        if not line:
            continue
        if line[0] == 'dataset' and line[1] == 'parameterName':
            headers = line
        else:
            key = line[7].replace(' ','_').replace('/','_').replace('+','').replace('(','').replace(')','').replace(',','_').replace('-','_').replace("'",'').replace('"','').replace('<','&lt;').replace('>','&gt;')   # structured comment name
            if key == 'lat':
                key='latitude'
            if key == 'lon' or key == 'long':
                key='longitude'
            parameterValue = remove_accents(line[2])
            dset = line[0]
            pj = line[5]
            if dset in TMP_METADATA_ITEMS:
                TMP_METADATA_ITEMS[dset][key] = parameterValue
            else:
                TMP_METADATA_ITEMS[dset] = {}
                TMP_METADATA_ITEMS[dset][key] = parameterValue


    # now get the data from just the datasets we have in CONFIG.ini
    for ds in CONFIG_ITEMS['datasets']:
        #print ds
        try:
          did = str(DATASET_ID_BY_NAME[ds])
        # except KeyError:
        #   insert_dataset(CONFIG_ITEMS, args)
        except:
          raise
          
        if ds in TMP_METADATA_ITEMS:
            for key in TMP_METADATA_ITEMS[ds]:
                #print key

                if key in required_metadata_fields:
                    if did in REQ_METADATA_ITEMS:
                        REQ_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')
                    else:
                        REQ_METADATA_ITEMS[did]= {}
                        REQ_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')

                else:

                    if did in CUST_METADATA_ITEMS:
                        CUST_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')
                    else:
                        CUST_METADATA_ITEMS[did]= {}
                        CUST_METADATA_ITEMS[did][key] = TMP_METADATA_ITEMS[ds][key].replace('"','').replace("'",'')






def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', unicode(input_str.strip(), 'utf8'))
    res = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # print "res = "
    # print res
    return res
    
class Utils:
    def __init__(self):
        pass          
    
    def is_local(self):
        print os.uname()[1]
        dev_comps = ['ashipunova.mbl.edu', "as-macbook.home", "as-macbook.local", "Ashipunova.local", "Annas-MacBook-new.local", "Annas-MacBook.local"]
        if os.uname()[1] in dev_comps:
            return True
        else:
            return False
            
    def is_vamps(self):
        print os.uname()[1]
        dev_comps = ['bpcweb8','bpcweb7','bpcweb7.bpcservers.private', 'bpcweb8.bpcservers.private']
        if os.uname()[1] in dev_comps:
            return True
        else:
            return False
            
    def print_both(self, message):
        print message
        logging.debug(message)
    
class Mysql_util:
    """
    Connection to vamps or vampsdev

    Takes parameters from ~/.my.cnf_node, default host = "vampsdev", db="vamps2"
    if different use my_conn = Mysql_util(host, db)
    """
    def __init__(self, host="bpcweb7", db="vamps2"):
        self.utils  = Utils()        
        self.conn   = None
        self.cursor = None
        self.rows   = 0
        self.new_id = None
        self.lastrowid = None
        
        try:           
            self.utils.print_both("=" * 40)
            self.utils.print_both("host = " + str(host) + ", db = "  + str(db))
            self.utils.print_both("=" * 40)
#             print "=" * 40
#             print "host = " + str(host) + ", db = "  + str(db)            
#             print "=" * 40

            if self.utils.is_local():
              self.conn = MySQLdb.connect(host=host, db=db, read_default_file=os.path.expanduser("~/.my.cnf_local"))
            else:       
              self.conn = MySQLdb.connect(host=host, db=db, read_default_file=os.path.expanduser("~/.my.cnf_node"))
              # self.db = MySQLdb.connect(host="localhost", # your host, usually localhost
              #                          read_default_file="~/.my.cnf_node"  )
              # cur = db.cursor()
            self.cursor = self.conn.cursor()
            # self.escape = self.conn.escape()
                   
        except MySQLdb.Error, e:
            self.utils.print_both("Error %d: %s" % (e.args[0], e.args[1]))
            raise
        except:                       # catch everything
            self.utils.print_both("Unexpected:")
            self.utils.print_both(sys.exc_info()[0])
#             print "Unexpected:"         # handle unexpected exceptions
#             print sys.exc_info()[0]     # info about curr exception (type,value,traceback)
            raise                       # re-throw caught exception   

    def execute_fetch_select(self, sql):
        if self.cursor:
          try:
            # sql = self.conn.escape(sql)
            self.cursor.execute(sql)
            res = self.cursor.fetchall ()
          except:
            self.utils.print_both(("ERROR: query = %s") % sql)
            raise
          return res

    def execute_no_fetch(self, sql):
        if self.cursor:
            self.cursor.execute(sql)
            self.conn.commit()
#            if (self.conn.affected_rows()):
#            print dir(self.cursor)
            return self.cursor.lastrowid
#        logging.debug("rows = "  + str(self.rows))
       
    def get_all_name_id(self, table_name):
        id_name = table_name + '_id'
        my_sql  = """SELECT %s, %s FROM %s""" % (table_name, id_name, table_name)
        res     = self.execute_fetch_select(my_sql)
        if res:
          return res


        
class Old_vamps_data:
  """
    get data from csv files made from old vamps
    put data to vamps2
  """
  def __init__(self, args):
    self.mysql_util = Mysql_util(args.dbhost, args.NODE_DATABASE)
    
    # self.mysql_util = Mysql_util(mysql_conn)
    self.cursor = self.mysql_util.cursor
    self.dataset_id_by_name_dict = {}
    self.project_id_by_name_dict = {}
    self.make_dataset_by_name_dict()
    self.make_project_by_name_dict()
        
  def make_dataset_by_name_dict(self):
    datasets_w_ids = self.mysql_util.get_all_name_id('dataset')
    self.dataset_id_by_name_dict = dict(datasets_w_ids)

  def make_project_by_name_dict(self):
    projects_w_ids = self.mysql_util.get_all_name_id('project')
    self.project_id_by_name_dict = dict(projects_w_ids)

  def collect_datasets(self, seqs_file_lines):
    # datasets_w_ids
      # logging.debug("In collect_datasets, seqs_file_lines = ")
      # logging.debug(seqs_file_lines)
      CONFIG_ITEMS_datasets_set = set()
      for field_list in seqs_file_lines:
        # print "field_list[3] = "
        # print field_list[3]
        CONFIG_ITEMS_datasets_set.add(field_list[3])
      CONFIG_ITEMS['datasets'] = list(CONFIG_ITEMS_datasets_set)
      # print "In collect_datasets, CONFIG_ITEMS['datasets'] = %s" % CONFIG_ITEMS['datasets']

      # [['id', 'sequence', 'project', 'dataset', 'taxonomy', 'refhvr_ids', 'rank', 'seq_count', 'frequency', 'dis     17 tance', 'rep_id', 'project_dataset']

  def put_custom_metadata_a(self, CUST_METADATA_ITEMS):
      """
        create new table
      """
      logging.debug('starting put_custom_metadata')
      # TABLE-1 === custom_metadata_fields
      cust_keys_array = {}
      all_cust_keys = []  # to create new table
      logging.debug("CONFIG_ITEMS['datasets'] = ")
      logging.debug(CONFIG_ITEMS['datasets'])
      print "CUST_METADATA_ITEMS = "
      print CUST_METADATA_ITEMS
      for ds in CONFIG_ITEMS['datasets']:
          did = str(self.dataset_id_by_name_dict[ds])
          logging.debug("DATASET_ID_BY_NAME[ds] = ")
          logging.debug(did)

          cust_keys_array[did]=[]

          # if did in CUST_METADATA_ITEMS:
          try:
            for key in CUST_METADATA_ITEMS[did]:
                logging.debug("key in CUST_METADATA_ITEMS[did] = ")
                logging.debug(key)
                if key not in all_cust_keys:
                    all_cust_keys.append(key)
                if key not in cust_keys_array[did]:
                    cust_keys_array[did].append(key)
                # q2 = "INSERT IGNORE into custom_metadata_fields(project_id, field_name, field_type, example)"
                q2 = "INSERT IGNORE into custom_metadata_fields (project_id, field_name, field_type, example)"
                q2 += " VALUES("
                q2 += "'1',"
                # should be:
                # q2 += "'"+str(CONFIG_ITEMS['project_id'])+"',"
                """
                todo
                q2 += "'"+str(CONFIG_ITEMS['project_id'])+"',"
                make dict for all
                if args.add_project:

                    q = "SELECT project_id from project where project='%s'" % (args.project)
                    logging.debug(q)
                    cur.execute(q)
                    mysql_conn.commit()
                    row = cur.fetchone()
                    CONFIG_ITEMS['project_id'] = row[0]
                    print("ADD TO PID="+str(CONFIG_ITEMS['project_id']))
                    logging.debug("ADDING to project -- PID="+str(CONFIG_ITEMS['project_id']))


                """
                q2 += "'"+str(key)+"',"
                q2 += "'varchar(128)'," #? are they alvays the same? couldn't they by numbers?
                q2 += "'"+str(CUST_METADATA_ITEMS[did][key])+"')"
                logging.debug("q2 = ")
                logging.debug(q2)
                print "q2 = "
                print q2
                self.cursor.execute(q2)
          except:
            raise
          
          # mysql_conn.commit()


if __name__ == '__main__':
    import argparse
    myusage = """
        -p/--project  project name          REQUIRED

        -s/--seqs_file       sequences file REQUIRED --FORMAT: see below
        -m/--metadata_file   metadata file  REQUIRED --FORMAT: see below

        -public/--public                    DEFAULT == '1'  true
        -env_source_id/--env_source_id      DEFAULT == '100' unknown
        -owner/--owner                      REQUIRED  (must be already in users table)
        -add/--add_project                  Will add to project

        Example project: ICM_AGW_Bv6
        Retrieve data from old_vams as csv files like this:
        >>METADATA:
            SPECIFIC for VAMPS headers in this format:
            dataset parameterName   parameterValue  units   miens_units project units_id    structured_comment_name method  other   notes   ts  entry_date  parameter_id    project_dataset
            if vamps project has no metadata, create a file with the above headers only.
            TAB delimited because QIIME/QIITA data comes TAB delimited
            TAB delimited and wrapped in double quotes:
            This metadatafile is NOT the same as from qiita
            mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_metadata where project='HMP_HP_v3v5';" |sed "s/'/\'/;s/\t/\"\t\"/g;s/^/\"/;s/$/\"/;s/\n//g"
            > metadata.csv

        >>SEQS:
            COMMA delimited and wrapped in double quotes
            mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences where project='AB_SAND_Bv6';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g"
            mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences_pipe where project='HMP_HP_v3v5';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g"
            > sequences.csv

        NOTE: the project and project_dataset fields in either file should not conflict with the new_vamps project name given on the command line.

    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)

    parser.add_argument("-p","--project",
                required=True,  action="store",   dest = "project", default='',
                help="""ProjectID""")


    parser.add_argument("-s","--seqs_file",
                required=True,  action="store",   dest = "seqs_file", default='',
                help="""file path""")
    parser.add_argument("-m","--metadata_file",
                required=True,  action="store",   dest = "metadata_file", default='',
                help="""file path""")
    parser.add_argument("-public","--public",
                 required=False,  action="store",   dest = "public", default='1',
                 help="""0 (private) or 1 (public)""")
    parser.add_argument("-env_source_id","--env_source_id",
                required=False,  action="store",   dest = "env_source_id", default='100',
                help="""EnvID from list""")
    parser.add_argument("-owner","--owner",
                required=True,  action="store",   dest = "owner",
                help="""VAMPS user name""")
    parser.add_argument("-delim","--delimiter",
                required=False,  action="store",   dest = "delim", default='tab',
                help="""METADATA: comma or tab""")
    parser.add_argument("-add","--add_project",
                required=False,  action="store_true",   dest = "add_project", default=False,
                help="""""")
    parser.add_argument("-host", "--host",          required=False,  action="store",   dest = "host", default='vampsdev',
                help="""database hostname: vamps or vampsdev  [default: vampsdev]""")   
    args = parser.parse_args()
    utils = Utils()        
    if args.host == 'vamps' or args.host == 'vampsdb':
        args.dbhost = 'vampsdb'
    elif args.host == 'vampsdev':
        args.dbhost = 'bpcweb7'
    args.NODE_DATABASE = 'vamps2'
    #args.database = 'vamps'
    
    
    #db = Mysql_util(host="localhost")

    # args.db = MySQLdb.connect(host=args.dbhost, db=args.NODE_DATABASE,
#                              read_default_file="~/.my.cnf"  )
#     args.cur = db.cursor()
    # cur.execute("SHOW databases like 'vamps%'")
#     dbs = []
#     db_str = ''
#     for i, row in enumerate(cur.fetchall()):
#         dbs.append(row[0])
#         db_str += str(i)+'-'+row[0]+';  '
#     print("db_str = ")
#     print(db_str)
    
    
    

    print 'DATABASE:',args.NODE_DATABASE
    print('See '+LOG_FILENAME)


    
    
    if args.project and args.seqs_file and args.metadata_file:
        pid = start(args)
        print "PID=", str(pid)
        print "Now Run: './taxcounts_metadata_files_utils.py -pid "+str(pid)+" -add' (-json_file_path; -host)"
        print "And re-start the server"
        logging.debug("Finished database_importer.py")
        logging.debug("Now Run: './taxcounts_metadata_files_utils.py -pid "+str(pid)+" -add' (-json_file_path; -host)")
        logging.debug("And re-start the server")
    else:
        print myusage


