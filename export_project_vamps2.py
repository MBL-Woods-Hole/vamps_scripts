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

import os
from stat import * # ST_SIZE etc
import sys
import shutil
import types
import random
import csv
from time import sleep
#sys.path.append( '/bioware/python/lib/python2.7/site-packages/' )
import pymysql as MySQLdb

import datetime







def start(args):
    """
        Function to load trimmed sequences into trimseq table.
        Input file is a '_clean' file: similar to a fasta but on one line:
        >30     pre-trimmed     0               unknown -	-	-	0	0	AAGGCTTGACATATAGAGGAAACGTCTGGAA
        >21     pre-trimmed     0               unknown -	-	-	0	0	TGGGCTCGAATGGTATTGGACTGCTGGTGAA
        Called from upload_file.php class
    """
    if args.site == 'vamps':
        db_host = 'vampsdb'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vamps/'
    elif args.site == 'vampscloud':
        db_host = 'vamps'
        args.NODE_DATABASE = 'vamps_development'
        db_home = '/usr/local/vamps/'
    else:
        db_host = 'vampsdev'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups-2/vampsweb/vampsdev/'
    print('MySQL - Connecting to', args.NODE_DATABASE,'on',db_host)
    mysql_conn = MySQLdb.connect(db = args.NODE_DATABASE, host=db_host, read_default_file=os.path.expanduser("~/.my.cnf_node")  )
    args.cur = mysql_conn.cursor()
    (args.project, args.datasets) = grab_project_n_datasets(args)
    
    print_project(args, args.project)
    print_datasets(args, args.project, args.datasets)
    grab_n_print_sequences(args, args.project, args.datasets)
    grab_n_print_metadata(args, args.project, args.datasets)
    grab_n_print_user(args, args.project, args.datasets)

def grab_project_n_datasets(args):
    
    q = "select project, title, project_description,  project_id, owner_user_id, dataset, dataset_description, dataset_id  from dataset join project using(project_id)" 
    if args.project:
        q += " where project='"+args.project+"'" 
    else:
        q += " where project_id='"+args.pid+"'"
    print(q)
    args.cur.execute(q)
    rows = args.cur.fetchall()
    dids = []
    datasets = [] 
     
    project = {}
    project['did_list'] = []  
    for row in rows:
        #print(row)        
        project['name'] = row[0]
        project['title'] = row[1]
        project['description'] = row[2]
        project['pid'] = row[3]
        project['oid'] = row[4]
        project['did_list'].append(str(row[7]))
        dataset = {}
        dataset['name'] = row[5]
        dataset['description'] = row[6]
        dataset['did'] = str(row[7])
        
        datasets.append(dataset)        
        
    return (project,datasets)
    
    
def print_project(args, project):
    f = open('project_'+project['name']+'.csv','w')
    f.write('"project","title","project_description","funding","env_sample_source_id","contact","email","institution"\n')
    f.write('"'+project['name']+'",')
    f.write('"'+project['title']+'",')
    f.write('"'+project['description']+'",')
    f.write('"unknown","unknown","unknown","unknown","unknown",\n')
    f.close()   

def print_datasets(args, project, datasets):
    f = open('dataset_'+project['name']+'.csv','w')
    f.write('"dataset","dataset_description","env_sample_source_id","project"\n')
    print(datasets)
    for ds in datasets:
        #print(ds)
        f.write('"'+ds['name']+'",')
        f.write('"'+ds['description']+'",')
        f.write('"unknown",')
        f.write('"'+project['name']+'",')
        f.write('\n')
    f.close()  

def grab_n_print_sequences(args, project, datasets):
    q = "select dataset,dataset_id,UNCOMPRESS(sequence_comp),gast_distance,rank,seq_count,concat_ws(';',domain,phylum,klass,`order`,family,genus,species,strain) as taxonomy from sequence_pdr_info"
    q += " join dataset using(dataset_id)"
    q += " join sequence_uniq_info using(sequence_id)"
    q += " join sequence using(sequence_id)"
    q += " join silva_taxonomy_info_per_seq using(silva_taxonomy_info_per_seq_id)"
    q += " join rank using(rank_id)"
    q += " join silva_taxonomy using(silva_taxonomy_id)"
    q += " join domain using(domain_id)"
    q += " join phylum using(phylum_id)"
    q += " join klass using(klass_id)"
    q += " join `order` using(order_id)"
    q += " join family using(family_id)"
    q += " join genus using(genus_id)"
    q += " join species using(species_id)"
    q += " join strain using(strain_id)"
    q += " where dataset_id in ("+','.join(project['did_list'])+")"
    print(q)
    # taxonomy remove trailing;;;;
    f = open('sequences_'+project['name']+'.csv','w')
    f.write('"id","sequence","project","dataset","taxonomy","refhvr_ids","rank","seq_count","frequency","distance","rep_id","project_dataset"\n')
    args.cur.execute(q)
    rows = args.cur.fetchall()
    for i,row in enumerate(rows):
        #print(i)
        #print(str(row[2]))
        f.write('"'+str(i+1)+'",')
        f.write('"'+(row[2]).decode('utf8')+'",')
        f.write('"'+project['name']+'",')
        f.write('"'+row[0]+'",')
        f.write('"'+(row[6]).strip(';')+'",')
        f.write('"unknown",')
        f.write('"'+row[4]+'",')
        f.write('"'+str(row[5])+'",')
        f.write('"unknown",')
        f.write('"'+str(row[3])+'",')
        f.write('"unknown",')
        f.write('"'+project['name']+'--'+row[0]+'",\n')
     
    f.close() 

def grab_n_print_metadata(args, project, datasets):
    f = open('metadata_'+project['name']+'.csv','w')
    f.write('"dataset","parameterName","parameterValue","units","miens_units","project","units_id","structured_comment_name","method","other","notes","ts","entry_date","parameter_id","project_dataset"\n')
    # check if table exists: custom_metadata_+project['pid']
    q1 = "select * from required_metadata_info"
    q1 += " where dataset_id in ("+','.join(project['did_list'])+")"
    args.cur.execute(q1)
    rows = args.cur.fetchall()
    for row in rows:
        print(row)
        
    custom_table = "custom_metadata_"+str(project['pid'])
    q2 = 'Show tables like "'+custom_table+'"'
    print(q2)
    args.cur.execute(q2)
    if args.cur.rowcount > 0:
        q3 = "select * from "+custom_table
        print(q3)
        rows = args.cur.fetchall()
        for row in rows:
            print(row)
    else:
        print('no custom table')   
    f.close() 
    
    
def grab_n_print_user(args, project, datasets):
    f = open('user_contact_'+project['name']+'.csv','w')
    f.write('"contact","username","email","institution","first_name","last_name","active","security_level","encrypted_password"\n')
    
    f.close() 
if __name__ == '__main__':
    import argparse

    # DEFAULTS
    site = 'vampsdev'
    user = ''



    data_object = {}
    file_type = "fasta"
    seq_file = ""
    sep = "--"
    dna_region = 'v6'
    project = ""
    dataset = ""

    myusage = """usage: vamps_load.py [options]

         Load user sequences into the database

         where
           
            -p, --project       The name of the project.   [optional]
                    Either Or
            -pid, --project_id    The name of the project.   [optional]
            -s, --site            vamps or vampsdev.
                                [default: vampsdev]



    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)
    parser.add_argument('-p', '--project',       required=False, action="store",   dest = "project",default='',
                                                    help = '')

    parser.add_argument('-pid', '--project_id',       required=False, action="store",   dest = "pid",default='',
                                                    help = '')
    parser.add_argument("-s","-site",        required=False,  action="store",   dest = "site", default='vamps',
                                                    help="""""")
    

    
    
    if len(sys.argv[1:])==0:
        print(myusage)
        sys.exit()
    args = parser.parse_args()
    if args.project and args.pid:
        print('Need Either Project or PID not both')
        sys.exit()
    if not args.project and not args.pid:
        print('Need Either Project or PID')
        sys.exit()
    

    start(args)
