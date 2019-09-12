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
import MySQLdb
import ConMySQL
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


def read_uc_file(args):
    
    size = float(args.size)
    #print "cluster size: "+str(size)
    uc_file = args.uclustfile
    f = open(uc_file)
    ref_running_total={}
    seqs=[]
    counter=1
    out=48
    cluster_num=1
    cluster={}
    for line in f:
        if counter > out:
            sys.exit("done: "+str(out))
        line = line.strip().split()
        if line[0] == 'H':
            print cluster_num,line
            ref_id = line[9]
            read_id = line[8]
            #seqs.append({'ref_id':line[9],'read_id':line[8]})
            found_size=float(line[3])
            if found_size < size or found_size > 100:
                print str(size)+" found size outside range: "+found_size
                sys.exit();
            
            
            
            cluster_str="Cluster_"+str(cluster_num)    
            if ref_id in ref_running_total:
                ref_running_total[ref_id].append(cluster_str)
            else:
                ref_running_total[ref_id]=[cluster_str]
            counter+=1
            
            
            if counter==1:
                
                cluster[cluster_str]={'ref_id':line[9],'read_id':line[8]}
                #advance cluster_num
                cluster_num+=1
            else:
                
                if len(ref_running_total[ref_id]) == 1:   # matching more than one seq in db so far
                    # new cluster
                    
                    cluster[cluster_str]={'ref_id':line[9],'read_id':line[8]}
                    #advance cluster_num
                    cluster_num+=1                       
                    
                else:
                    # assign this seq to largest cluster
                    for cluster_id in ref_running_total[ref_id]:
                        print cluster_id
                    # do not advance cluster number
            
            
            
            
    
        
        
        
    f.close()
    
    

    
  
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
                                              
    #parser.add_argument("-r", "--ref_database", required=True,  action="store",   dest = "ref_db",
    #                                                help="run number")  
                                                    
    #parser.add_argument("-fa", "--fastafile",  required=True,  action="store",   dest = "in_fastafile", 
    #                                                help="Fasta File ") 
    parser.add_argument("-uc", "--uclustfile",  required=True,  action="store",   dest = "uclustfile", 
                                                    help="Fasta File ")                                 
    parser.add_argument('-s', '--size',         required=True,   action="store",   dest = "size",        
                                                 help = 'cluster size (0.97)')                                                 
    #parser.add_argument("-o", "--out_file",    required=False, action="store",   dest="out_file", default='outfile',
    #                                             help="don't print any status messages to stdout")
    
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
    
    read_uc_file(args)
    
    