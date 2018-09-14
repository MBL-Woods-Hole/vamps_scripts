#!/usr/bin/env python

##!/usr/local/www/vamps/software/python/bin/python
##!/usr/bin/env python
##!/usr/local/epd_python/bin/python
##!/bioware/python/bin/python
##!/usr/bin/env python
##!/usr/local/www/vamps/software/python/bin/python
###!/usr/bin/env python

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
sys.path.append( '/bioware/python/lib/python2.7/site-packages/' )
import pymysql as MySQLdb
from apps.ConMySQL import Conn
import datetime

sys.path.append("/bioware/pythonmodules/illumina-utils/")
import fastqlib
from vamps_utils import FastaReader





def load_trimmed_seqs(myobject):
    """
        Function to load trimmed sequences into trimseq table.
        Input file is a '_clean' file: similar to a fasta but on one line:
        >30     pre-trimmed     0               unknown -	-	-	0	0	AAGGCTTGACATATAGAGGAAACGTCTGGAA
        >21     pre-trimmed     0               unknown -	-	-	0	0	TGGGCTCGAATGGTATTGGACTGCTGGTGAA
        Called from upload_file.php class
    """
    infile  = myobject['infile']
    if not os.path.exists(infile):
        print("Could not find input file - Exiting")
        sys.exit(102)
    # should be seqfile.fa_clean
    project = myobject['project']
    dataset = myobject['dataset']
    dna_region  = myobject['dna_region']
    runcode = myobject['runcode']
    user    = myobject['user']
    date    = myobject['datetime']
    connobj  = myobject['obj']
    db = connobj.get_conn()
    cursor = connobj.get_cursor()
    domain  = myobject['domain']
    file_base = myobject['file_base']
    status_file = os.path.join(file_base,'STATUS.txt')
    print(status_file)
    fh = open(status_file,'a')
    execute_error = False
    if not domain:
        if dna_region == 'v9' or dna_region == 'its1':
            domain = 'eukarya'
        elif dna_region[-1:] == 'a':
            domain = 'archaea'
            dna_region = dna_region[:-1]
        else:
            domain = 'bacteria'


    insert_table = 'vamps_upload_trimseq';
    loadquery =  "load data local infile '"+infile+" '\n"
    loadquery += " replace into table "+insert_table+" \n"
    loadquery += " fields terminated by '\t' "
    #loadquery += " lines starting by '>'           \n"
    loadquery += " set entry_date = '"+date+"',     \n"
    loadquery += " source = '"+dna_region+"',       \n"
    loadquery += " length = char_length(sequence),  \n"
    loadquery += " project = '"+project+"',         \n"
    loadquery += " dataset = '"+dataset+"',         \n"
    loadquery += " run = '"+runcode+"',             \n"
    loadquery += " user = '"+user+"',               \n"
    loadquery += " domain = '"+domain+"',           \n"
    loadquery += " countN = '0',                    \n"
    loadquery += " delete_reason = ''"
    print(loadquery)

    try:
        cursor.execute(loadquery)
    except:
        db.rollback()
        execute_error=True
    else:
        db.commit()

    if execute_error:
        fh.write("{\"status\": \"ERROR\", \"Sequence Load Error\": True}\n")
    else:
        fh.write("{\"status\": \"SUCCESS\", \"Sequence Load Success\": True}\n")
    fh.close()


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
            -i, --infile The name of the input file file.  [required]

            -t, --type    raw or trim.   [required]

            -p, --project    The name of the project.   [optional]

            -d, --dataset     The name of the dataset.
                                [default: unknown]
            -dna_region

            --site            vamps or vampsdev.
                                [default: vampsdev]
            -r, -runcode      runcode
                                [default: random number]
            -u, --user



    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)
    parser.add_argument('-i', '--infile',       required=True, action="store",   dest = "infile",
                                                    help = '')
    parser.add_argument("-t", "--upload_type",         required=True,  action="store",   dest = "type",
                                                    help="raw or trimmed")



    parser.add_argument("-site",                required=True,  action="store",   dest = "site",
                                                    help="""""")
    parser.add_argument("-r", "--runcode",      required=True,  action="store",   dest = "runcode",
                                                    help="""""")

    parser.add_argument("-u", "--user",         required=True,  action="store",   dest = "user",
                                                    help="user name")
    parser.add_argument("-file_type",           required=True,  action="store",   dest = "file_type",
                                                    help="sff, fasta or fastq")
    parser.add_argument('-file_base',               required=True, action="store",   dest = "file_base",
                                                    help = 'where the files are loacated')


    if len(sys.argv[1:])==0:
        print(myusage)
        sys.exit()
    args = parser.parse_args()

    data_object['infile'] = args.infile
    data_object['datetime'] = str(datetime.date.today())
    data_object['type'] = args.type
    data_object['runcode'] = args.runcode
    data_object['site'] =  args.site
    data_object['user'] =  args.user
    data_object['file_base'] =  args.file_base
    data_object['file_type'] =  args.file_type








    if data_object['site'] == 'vamps':
        #db_host = 'vampsdb'
        db_host = 'bpcdb2'
        #db_name = 'vamps'
        db_name = 'vamps_user_uploads'
        db_home = '/xraid2-2/vampsweb/vamps/'
    else:
        #db_host = 'vampsdev'
        db_host = 'bpcdb2'
        #db_name = 'vamps'
        db_name = 'vampsdev_user_uploads'
        db_home = '/xraid2-2/vampsweb/vampsdev/'

    print(db_host, db_name, db_home)
    obj=Conn(db_host, db_name, db_home)
    #c = obj.get_cursor()
    data_object['obj'] = obj


    load_trimmed_seqs(data_object)
