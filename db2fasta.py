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
import csv, json
import configparser as ConfigParser

import datetime
today = str(datetime.date.today())
import subprocess
import pymysql as MySQLdb

"""

"""
# Global:

    
def start(args):
    #  https://www.ncbi.nlm.nih.gov/books/NBK279688/
    sqlQuery = "SELECT sequence_id, project, dataset, SUM(seq_count) as knt, UNCOMPRESS(sequence_comp) as sequence" 
    if args.tax:
        sqlQuery += ", concat_ws(';',domain,phylum,klass,`order`,family,genus,species) as tax"
    if args.latlon:
        sqlQuery += ", latitude,longitude"
    sqlQuery += " FROM sequence" 
    sqlQuery += " JOIN sequence_pdr_info using(sequence_id)"
    sqlQuery += " JOIN dataset using(dataset_id)"
    sqlQuery += " JOIN project using(project_id) "
    if args.latlon:
        sqlQuery += " JOIN required_metadata_info using(dataset_id) "
    if args.tax:
        sqlQuery += " JOIN silva_taxonomy_info_per_seq using(sequence_id)" 
        sqlQuery += " JOIN silva_taxonomy using(silva_taxonomy_id)"
        sqlQuery += " JOIN domain on(domain.domain_id=silva_taxonomy.domain_id)"
        sqlQuery += " JOIN phylum using(phylum_id)"
        sqlQuery += " JOIN klass using(klass_id)"
        sqlQuery += " JOIN `order` using(order_id)"
        sqlQuery += " JOIN family using(family_id)"
        sqlQuery += " JOIN genus using(genus_id)"
        sqlQuery += " JOIN species using(species_id) "

    if args.sql_where:
        sqlQuery += args.sql_where
    elif args.project:        
        sqlQuery += "WHERE project='"+args.project+"'"
    else:
        sys.exit('either enter a project(-p) or where clause(-sql)')
    sqlQuery += " GROUP BY project, dataset, sequence"
    if args.limit:
        sqlQuery += " limit "+args.limit
    print(' ')
    print(sqlQuery)
    cur.execute(sqlQuery) 
    if cur.rowcount == 0:
        sys.exit('Nothing Returned -> Exiting')
    rows = cur.fetchall() 
    fp = open(args.out_file_name,'w')
    fa_rowcount=0
    for row in rows:
        seqid = str(row['sequence_id'])
        project = row['project']
        dataset=row['dataset']
        freq = row['knt']
        seq = str(row['sequence'],'utf8')
        pjds = project+'--'+dataset
        #id = '>'+seqid+' project='+project+'|dataset='+dataset+'|frequency='+str(freq)
        # id formatting from https://www.ncbi.nlm.nih.gov/books/NBK279688/
        id = '>'+pjds+'|'+seqid
        
        if args.tax:
            id += '|'+row['tax'].rstrip(';')
        if args.latlon:
            if row['latitude'] and row['longitude']:
                id += '|'+str(row['latitude'])+';'+str(row['longitude'])
            else:
                id += '|none;none'
        id += '|frequency:'+str(freq)
        
        if args.expand:
            expand_count = 1
            for n in range(freq):
                new_seqid = seqid+'_'+str(expand_count)
                #id = '>'+new_seqid+' project='+project+'|dataset='+dataset
                id = '>'+pjds+'|'+new_seqid
                fp.write(id+'\n')
                fp.write(seq+'\n')
                expand_count += 1
                fa_rowcount += 1
        else:   
            fp.write(id+'\n')
            fp.write(seq+'\n')
            fa_rowcount += 1
    fp.close()
    print(args.out_file_name)
    if args.expand:
        print('Sequence Count:',fa_rowcount,'Expanded')
    else:
        print('Sequence Count:',fa_rowcount,'Uniqued')
        
            

def delete_metadata_only(args):
    pass
    
    
def delete_dids_from_metadata_bulk_file(args):
    pass

if __name__ == '__main__':
    import argparse
    
    
    myusage = """ For New VAMPS
    
    Usage:  db2fasta.py -host vampsdev -p AB_SAND_Bv6
         db2fasta.py -host vamps  -sql "WHERE dataset_id=\'12342\'"
         db2fasta.py -host vamps  -sql "WHERE project like \'ICM%%\'"
         
         For VAMPS2 Blast databases see: /groups/vampsweb/vamps/nodejs/blast/README
         db2fasta.py -host vamps -tax -sql "where public='1' and project like '%Ev9'" -o Ev9

  Options:  
            -host/--host      hostname [default = jbpcdb]
            -id     id field name [default = read_id]
            -seq    sequence field name [default = sequence]
            -o      output fasta file
            -ex/--expand tag will add multiple sequences where the frequency is greater than 1
            
                seqid without -ex flag
                    '>seqid|project=pj|dataset=ds|frequency=freq'
                seqid with -ex flag:
                    '>seqid_x|project=pj|dataset=ds'
                    where x is 1,2,3...
                    
            Use EITHER project OR WHERE Clause
            -proj/--project
            -sql/--sql_where    sql WHERE statement if you want do use join or where clause
                                Inclose the clause in double quotes and Include the WHERE keyword
                                "WHERE public='1'"
                                "WHERE project like 'ICM%%'"
                                "WHERE dataset_id='12342'"
            -tax/--tax  Include taxonomy in defile (for blast databases) default: False


 db2fasta - exports sequence information from a database to a fasta file.  
            All records in a table may be exported, specifying the table, 
            id field and sequence data field.  
            Or, an sql select statement can be used to export a subset
            of the sequence data.

            Inclusion of the -rc flag will return the reverse complement 
            of the sequences.
			
            The -q flag warns db2fasta that the output sequences are quality
            scores not nucleotide sequences.


    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)                 
    
    
    
    parser.add_argument("-host", "--host",          
                required=True,  action='store', dest = "hostname", 
                help="vamps or vampsdev ") 
    # parser.add_argument("-id", "--id",          
#                 required=False,  action='store', dest = "id", default='sequence_id',
#                 help=" ") 
#     parser.add_argument("-seq", "--sequence",          
#                 required=False,  action='store', dest = "sequence", default='sequence',
#                 help=" ")           
    parser.add_argument("-ex", "--expand",          
                required=False,  action='store_true', dest = "expand", default=False,
                help=" ")
    parser.add_argument("-o", "--out_file_name",          
                required=False,  action='store', dest = "out_file_name", default='outfile.fa',
                help=" ")  
    parser.add_argument("-sql", "--sql_where",          
                required=False,  action='store', dest = "sql_where", default='',
                help=" Include 'WHERE'")  
    parser.add_argument("-proj", "--project",          
                required=False,  action='store', dest = "project", default='',
                help=" ")  
    parser.add_argument("-tax", "--tax",          
                required=False,  action='store_true', dest = "tax", default=False,
                help="Include GAST taxonomy in defline")
    parser.add_argument("-latlon", "--latlon",          
                required=False,  action='store_true', dest = "latlon", default=False,
                help="Include lat--lon in defline")
    parser.add_argument("-limit", "--limit",          
                required=False,  action='store', dest = "limit", default='',
                help="Include lat--lon in defline")                 
    
    args = parser.parse_args()    
    if args.project and args.sql_where:
        print('Found both Project AND SQL: will use project only')
    
    if args.hostname == 'vamps':
        db_host = 'vampsdb'
        #db_host = 'bpcweb8'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vamps/'
    elif args.hostname == 'vampsdev':
        #db_host = 'vampsdev'
        db_host = 'bpcweb7'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vampsdev/'
    else:
        db_host = 'localhost'
        db_home = '~/'
        args.NODE_DATABASE = 'vamps_development'
    
    args.obj = MySQLdb.connect( host=db_host, db=args.NODE_DATABASE, read_default_file=os.path.expanduser("~/.my.cnf_node")    )

    #db = MySQLdb.connect(host="localhost", # your host, usually localhost
    
    cur = args.obj.cursor(MySQLdb.cursors.DictCursor)

    start(args)
