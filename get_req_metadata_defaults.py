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

import datetime
import logging
today = str(datetime.date.today())
import subprocess
import pymysql as MySQLdb
import pprint
pp = pprint.PrettyPrinter(indent=4)

# Global:
 # SUMMED_TAX_COLLECTOR[ds][rank][tax_string] = count

# ranks =['domain','phylum','klass','order','family','genus','species','strain']
# tax_ids = ['domain_id','phylum_id','klass_id','order_id','family_id','genus_id','species_id','strain_id']
# accepted_domains = ['bacteria','archaea','eukarya','fungi','organelle','unknown']
# required_metadata = []
queries = [
    {"table":"term","query": "SELECT term_id FROM term WHERE term_name = 'unknown'"},
    {"table":"dna_region","query": "SELECT dna_region_id FROM dna_region WHERE dna_region = 'unknown'"},
    {"table":"adapter_sequence","query": "SELECT run_key_id FROM run_key WHERE run_key = 'unknown'"},   # adapter_sequence
    {"table":"sequencing_platform","query": "SELECT sequencing_platform_id FROM sequencing_platform WHERE sequencing_platform = 'unknown'"},
    {"table":"target_gene","query": "SELECT target_gene_id FROM target_gene WHERE target_gene = 'unknown'"},
    {"table":"domain","query": "SELECT domain_id FROM domain WHERE domain = 'unknown'"},
    {"table":"illumina_index","query": "SELECT illumina_index_id FROM illumina_index WHERE illumina_index = 'unknown'"},
    {"table":"primer_suite","query": "SELECT primer_suite_id FROM primer_suite WHERE primer_suite = 'unknown'"},
    {"table":"run","query": "SELECT run_id FROM run WHERE run = 'unknown'"},
    {"table":"env_package","query": "SELECT env_package_id FROM env_package WHERE env_package = 'unknown'"}
]
class Unknowns:

    def __init__(self, dbhost = "bpcweb7", dbname = "vamps2", read_default_file = os.path.expanduser("~/.my.cnf_node")):
        self.mysql_conn = MySQLdb.connect(db = dbname, host=dbhost, read_default_file=os.path.expanduser("~/.my.cnf_node")  )
        self.cur = self.mysql_conn.cursor()
        
    def start(self):
        
        self.unknowns = {}
        
        for q in queries:
            self.cur.execute(q['query'])
            self.mysql_conn.commit()
            row = self.cur.fetchone()
            self.unknowns[q['table']] = row[0]
        
        print ('unknown IDs',self.unknowns)
       
    
                
            
        

if __name__ == '__main__':
    import argparse
    
    
    myusage = """usage: get_req_metadata_defaults.py  [options]
         
         USE: prints out the table IDs for the unknows for the following tables:
            term
            dna_region
            adaptor_sequence
            sequencing_platform
            target_gene
            domain
            illumina_index
            primer_suite
            run
            
        These IDs are useful to input in the script 'old_2_new_vamps_by_project.py' 
         
         where
 
            -host/--host   [Default:local]
                vamps vampsdev or localhost

           

"""

    parser = argparse.ArgumentParser(description="" ,usage=myusage)                     
    
    parser.add_argument("-host", "--host",    
                required=False,  action='store',  dest = "host",  default='local',
                help="vamps, vampsdev or localhost (default)")            
              
    
    args = parser.parse_args() 

    # get_pids
    if args.host == 'vamps' or args.host == 'vampsdb':
        hostname = 'vampsdb'
        NODE_DATABASE = 'vamps2'
    elif args.host == 'vampsdev':
        hostname = 'vampsdev'
        NODE_DATABASE = 'vamps2'
    else:
        hostname = 'localhost'
        NODE_DATABASE = 'vamps_development'
    
    print ('HOST',hostname )   

    
    # socket=/tmp/mysql.sock
    
    
    U = Unknowns(hostname, NODE_DATABASE,'/Users/avoorhis/.my.cnf_node')
    U.start()


