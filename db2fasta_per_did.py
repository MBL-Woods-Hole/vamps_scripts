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
#from stat import * # ST_SIZE etc
import sys
import configparser as ConfigParser
from time import sleep
from os.path import expanduser
import datetime
import subprocess as subp
import gzip, csv, json
import pymysql as MySQLdb


def get_fasta_sql(args):
    sql = "SELECT UNCOMPRESS(sequence_comp) as sequence, sequence_id, seq_count, project, dataset from sequence_pdr_info\n"
    sql += " JOIN sequence using (sequence_id)\n"
    sql += " JOIN dataset using (dataset_id)\n"
    sql += " JOIN project using (project_id)\n"
    sql += " where dataset_id in ('"+args.did+"')"
    return sql


    
# def write_file_txt(args, out_file, file_txt):
# 
#     print(out_file)
#     
#     with open(out_file, 'wb') as f:
#         f.write(file_txt)
import sqlite3
from sqlite3 import Error
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    return conn
            
def sqlite_conn(args):
    
    database = '/groups/vampsweb/new_vamps_maintenance_scripts/data/dbfile.db'
    sql_create_seqs_table = """ CREATE TABLE IF NOT EXISTS sequences (
                                        id integer PRIMARY KEY,
                                        seqid text NOT NULL,
                                        seq text NOT NULL
                                    ); """
    conn = create_connection(database)
    # create tables
    if conn is not None:
        # create projects table
        try:
            c = conn.cursor()
            #c.execute(sql_create_seqs_table)
        except Error as e:
            print(e)
    else:
        print("Error! cannot create the database connection.")
    return conn
    
    
def run(args):
    conn = sqlite_conn(args)
    
    cur = conn.cursor()
    sql = get_fasta_sql(args)
    dblitesql = "INSERT INTO sequences(seqid, seq) VALUES('one','two')"
    cur.execute(dblitesql)
    print(cur.lastrowid)
    print(sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    #full_file = '/groups/vampsweb/new_vamps_maintenance_scripts/data/avoorhis-12345678_export-working-dir/fasta.fa'
    #fpf = open(full_file, 'a')
    dataset_name_collector = {}
    fp = open(args.outfasta, 'w')
    for i,row in enumerate(rows):
        #print(row)
        pjds = row['project']+'--'+row['dataset']
        seq_count = row['seq_count']
        seqid = '>'+str(row['sequence_id'])+'|'+pjds+'|frequency:'+str(seq_count)
        seq = row['sequence'].decode()
        
        fp.write(seqid+'\n')
        fp.write(seq+'\n')
        
        
        #fpf.write('>'+str(row['sequence_id'])+'|'+pjds+'|frequency:'+str(seq_count)+'\n')
        #fpf.write(seq+'\n')
        
    fp.close()
    #fpf.close()


        
if __name__ == '__main__':

    import argparse


    myusage = """usage: db2fasta_av.py  [options]


         where

            -host/--host      vamps or [default: vampsdev]

            -d/--did,       did dataset_id
            -o/--outfasta   fasta file output

           

    """
    parser = argparse.ArgumentParser(description = "", usage = myusage)

    parser.add_argument("-host", "--host",     required=True,  action="store",   dest = "host",
                                                    help="""database hostname: vamps or vampsdev or local(host) [default: vampsdev]""")
    parser.add_argument("-d", "--did",      required=True,  action="store",   dest = "did",
                                                    help="")
    parser.add_argument("-o", "--outfasta",   required=True,  action="store",   dest = "outfasta",
                                                    help="")
  
    args = parser.parse_args()

    args.today = str(datetime.date.today())

    if args.host == 'vamps':
        db_host = 'vampsdb'
        #db_host = 'bpcweb8'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vamps/'
        #args.files_home ='/groups/vampsweb/vamps/nodejs/json/vamps2--datasets_silva119/'
    elif args.host == 'vampsdev':
        db_host = 'bpcweb7'
        #db_host = 'bpcweb7'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vampsdev/'
        #args.files_home ='/groups/vampsweb/vampsdev/nodejs/json/vamps2--datasets_silva119/'
    else:
        db_host = 'localhost'
        db_home = '~/'
        #args.files_home ='public/json/vamps_development--datasets_silva119/'
    db_name = args.NODE_DATABASE


    print (db_host, db_name)

    home = expanduser("~")
    #print(home)
    args.obj = MySQLdb.connect( host=db_host, db=db_name, read_default_file=home+'/.my.cnf_node', cursorclass=MySQLdb.cursors.DictCursor    )
    cursor = args.obj.cursor()
    
    
    run(args)
    

