#!/usr/bin/env python

##!/usr/local/www/vamps/software/python/bin/python

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

import csv
import json
from time import sleep
#import MySQLdb
#import ConMySQL
import datetime
sys.path.append( '/bioware/pythonmodules/' )
from fastalib.fastalib import SequenceSource, FastaOutput
# from pipeline.utils import *
# from pipeline.sample import Sample
# from pipeline.runconfig import RunConfig
# from pipeline.run import Run
# from pipeline.trim_run import TrimRun
# from pipeline.chimera import Chimera
# from pipeline.gast import Gast
# from pipeline.vamps import Vamps
# from pipeline.fasta_mbl_pipeline import MBLPipelineFastaUtils
ranks = {
    1:'domain',
    2:'phylum',
    3:'class',
    4:'order',
    5:'family',
    6:'genus',
    7:'species',
    8:'strain'
}
today = datetime.date.today()

class FastaReader:
    def __init__(self,file_name=None):
        self.file_name = file_name
        self.h = open(self.file_name)
        self.seq = ''
        self.id = None
        self.revcomp_seq = None
        self.base_counts = None

    def next(self): 
        def read_id():
            return self.h.readline().strip()[1:]

        def read_seq():
            ret = ''
            while True:
                line = self.h.readline()
                
                while len(line) and not len(line.strip()):
                    # found empty line(s)
                    line = self.h.readline()
                
                if not len(line):
                    # EOF
                    break
                
                if line.startswith('>'):
                    # found new defline: move back to the start
                    self.h.seek(-len(line), os.SEEK_CUR)
                    break
                    
                else:
                    ret += line.strip()
                    
            return ret
        
        self.id = read_id()
        self.seq = read_seq()
        
        
        if self.id:
            return True    


def uclust(args):

    sorted_fastafile = args.out_file
    ucfile = args.out_file + ".uc"
    ref_db_file = args.ref_db
    f_in = SequenceSource(args.in_fastafile, unique=True)
    f_out = FastaOutput(sorted_fastafile)
    # create fasta file sorted from highest to lowest frequency
    while f_in.next():
        f_out.store(f_in, split = False)
    
    # QIIME:
    # uclust --maxrejects 500 --input /tmp/UclustExactMatchFilterIGAHlpYeOoAzTuW7FBVl.fasta --id 0.97 --w 12 --stepwords 20 
    #       --usersort --maxaccepts 20 --libonly --stable_sort --lib /xraid2-2/vampsweb/vampsdev/otus/gg_97_otus_12_10.fasta 
    #       --uc /xraid2-2/vampsweb/vampsdev/otus/avoorhis_85537519//uclust_ref_picked_otus/uclust_ref_clusters.uc
    # Eqivalent with usearch6?
    # usearch6 -usearch_global myoutfile.fa -db ../gg_97_otus_12_10.fasta -id 0.97 -strand plus -maxaccepts 8 -maxrejects 256 -uc out.uc
    #  OR
    # /bioware/uclust/uclust --query myoutfile.fa --db ../gg_97_otus_12_10.fasta --id 0.97 --w 12 --stepwords 20 
    # --maxaccepts 20 --maxrejects 500 --libonly --uc out_8.uc
    uclust_cmd = ["/bioware/uclust/uclust"]
    uclust_cmd.append("--query")
    uclust_cmd.append(sorted_fastafile)
    uclust_cmd.append("--db")
    uclust_cmd.append(ref_db_file)
    uclust_cmd.append("--id")
    uclust_cmd.append("0.97")
    uclust_cmd.append("--w")
    uclust_cmd.append("12")
    uclust_cmd.append("--stepwords")
    uclust_cmd.append("20")
    uclust_cmd.append("--maxaccepts")
    uclust_cmd.append("20")
    uclust_cmd.append("--maxrejects")
    uclust_cmd.append("500")
    uclust_cmd.append("--libonly")
    uclust_cmd.append("--uc")
    uclust_cmd.append(ucfile)
    
    print uclust_cmd
    from subprocess import call
    #call(uclust_cmd)
    datetime = args.today
    
    
 
def load_db(cursor, datasets, otus, matrix, ds_counts, taxonomy, project, user, run, otu_count):
    ds_cluster_count={}
    for n,otu_name in enumerate(otus):
            cluster = otu_name
            
            for m,ds_name in enumerate(datasets):
            
                if ds_name in ds_cluster_count:
                
                    if otu_name in ds_cluster_count[ds_name]:
                        ds_cluster_count[ds_name][otu_name] += 1
                    else:
                        ds_cluster_count[ds_name][otu_name] = 1
                else:                
                      
                    ds_cluster_count[ds_name] = {}
            
                #ds_cluster_count[ds_name]
                if ds_name in ds_counts:
                    dataset_count = ds_counts[ds_name]
                else:
                    dataset_count = 0
                if matrix[n][m]:  # do not enter the zeros
                    
                    knt     = matrix[n][m]
                    if dataset_count:
                        frequency = float(knt)/dataset_count
                    else:
                        frequency = 0
                     
                    #print knt,dataset_count,frequency
                    tax = taxonomy[n]
                    rank = ranks[len(tax.split(';'))]
                    project_dataset = project+'--'+ds_name
                    
                    query1 = "insert into otu_user_clusters ( \
                        upload_id,cluster,project,dataset,knt,dataset_count,frequency,taxonomy,rank,project_dataset,user,domain,otu_size \
                        ) values ( '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )" % \
                            (run,         cluster,    project,    ds_name,    knt,      
                            dataset_count,  frequency,  tax,        rank,       project_dataset,    
                            user,           'unknown',      'unknown'  
                            )
                    #print query
                    try:
                        cursor.execute(query1)
                    except:
                        pass
    
    print 'rows '+str(n)
    sum_counts=0 
    dataset_names=[]
    for m,ds_name in enumerate(datasets):
        dataset_names.append(ds_name)
#         if ds_name in ds_counts:
#             dataset_count = ds_counts[ds_name]
#         else:
#             dataset_count = 0
#         sum_counts += dataset_count
#         print ds_name,ds_counts[ds_name]
        
        query2 = "insert into otu_projects_datasets (upload_id,project,dataset,dataset_info) \
                    values( '%s','%s','%s','%s')" % \
                    (run,project,ds_name,ds_name)                    
        try:
            cursor.execute(query2)
        except:
            pass
    description = 'From Biom File';    
    #description += '&lt;br&gt;&lt;br&gt;' + '&lt;br&gt;'.join(dataset_names)
    title='From Biom File'
    
    query3 = "insert into otu_project_info (upload_id,project,title,description,user,upload_date,otu_size,domain,dna_region,method,otu_count) \
                    values( '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                    (run,project,title,description,user,today,'unknown','unknown','unknown','unknown',str(otu_count))
                    
    try:
        cursor.execute(query3)
    except:
        pass
  
if __name__ == '__main__':
    import argparse
    
    # DEFAULTS
    site = 'vampsdev'
    
    user = ''  
    
    data_object = {}
    
    

    
    myusage = """usage: otu_utils.py  [options]
         
         
         
         where options:
            -fa, --fastafile The name of the fasta file.  [required]
            
            
            
            -m, --method     Has to be either uclust_ref, usearch, crop or slp. 
                                [default: unknown]
            
            -r, --ref_database     Reference Database Fasta file
                                [default: unknown]
            -o, --otu_file       output file name
                                [default: unknown]                   
                      
    
    
    """
    parser = argparse.ArgumentParser(description="Convert any fasta file to uniqued and sorted. Sorts by decreasing abundance." ,usage=myusage)
                                              
    
                                                        
    #parser.add_argument("-site", "--db_host",      required=False,  action="store",   dest = "site", default='vampsdev',
    #                                                help="""database hostname: vamps or vampsdev
    #                                                    [default: vampsdev]""")  
    
    #parser.add_argument("-u", "--user",         required=False,  action="store",   dest = "user", 
    #                                                help="user name")  
                                              
    parser.add_argument("-r", "--ref_database", required=True,  action="store",   dest = "ref_db",
                                                    help="run number")  
                                                    
    parser.add_argument("-fa", "--fastafile",  required=True,  action="store",   dest = "in_fastafile", 
                                                    help="Fasta File ") 
                                    
    parser.add_argument('-m', '--method',  required=True,   action="store",   dest = "method",        
                                                 help = 'Method to use: uclust_ref')                                                 
    parser.add_argument("-o", "--out_file",    required=False, action="store",   dest="out_file", default='outfile',
                                                 help="don't print any status messages to stdout")
    
    args = parser.parse_args()
    
    args.today = str(datetime.date.today())
    
    
    
    
    
    
#     if args.site == 'vamps':
#         db_host = 'vampsdb'
#         db_name = 'vamps'
#         db_home = '/xraid2-2/vampsweb/vamps/'
#     else:
#         db_host = 'vampsdev'
#         db_name = 'vamps'
#         db_home = '/xraid2-2/vampsweb/vampsdev/'
#     
#     
#     obj=ConMySQL.New(db_host, db_name, db_home)
#     args.cursor = obj.get_cursor()
#     if args.user:
#         user = args.user
#     else:
#         user = obj.get_db_user()
#     args.user = user
#     
#     if args.run:
#         run = args.run
#     else:
#         import random
#         run = str(random.randint(1000000,9999999))
#     
#     args.run = run
    if args.method == 'uclust_ref':
        uclust(args)
    
    