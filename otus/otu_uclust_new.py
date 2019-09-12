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
import subprocess
import csv
import json
from time import sleep
#import MySQLdb
#import ConMySQL
import datetime
#sys.path.append( '/bioware/pythonmodules/' )
#from fastalib.fastalib import SequenceSource, FastaOutput
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


def start_uclust(args):
    """
     From otus_uc2mtx_vamps
     --cluster_fast $prefix.fa --sizeout --iddef 3 --id $pct_id --consout $prefix.cons.$pctid_int.fa --uc $prefix.otus.$pctid_int.uc --log $log_filename
     """
    # vsearch_cmd = "/groups/vampsweb/seqinfobin/vsearch"
#     vsearch_cmd += ' --cluster_fast'
#     vsearch_cmd += ' '+args.infile
#     vsearch_cmd += ' --sizeout'
#     vsearch_cmd += ' --iddef 3'
#     vsearch_cmd += ' --id 0.97'
#     vsearch_cmd += ' --consout'
#     vsearch_cmd += ' '+args.infile+'.97.fa'
#     vsearch_cmd += ' --uc'
#     vsearch_cmd += ' '+args.infile+'.97.uc'
#     vsearch_cmd += ' --log'
#     vsearch_cmd += ' '+args.infile+'.log'
    print("RUN Command:")
    print(vsearch_cmd)
    
    vsearch_cmd = ["/groups/vampsweb/seqinfobin/vsearch"]
    vsearch_cmd.append('--cluster_fast')
    vsearch_cmd.append(args.infile)
    vsearch_cmd.append('--sizeout')
    vsearch_cmd.append('--iddef')
    vsearch_cmd.append('3')
    vsearch_cmd.append('--id')
    vsearch_cmd.append('0.97')
    vsearch_cmd.append('--consout')
    vsearch_cmd.append(os.path.basename(args.infile)+'.97.fa')
    vsearch_cmd.append('--uc')
    vsearch_cmd.append(os.path.basename(args.infile)+'.97.uc')
    vsearch_cmd.append('--log')
    vsearch_cmd.append(os.path.basename(args.infile)+'.log')
    
    subprocess.call(vsearch_cmd)
def start_slp(args):
    """
    SLP (Single linkage pre-clustered) OTUs
    Used for short (singlehyper-variable region only) sequences only 
    Workflow:
    mothur:unique-seqs
    mothur:align-seqs   
    mothur:pre-cluster
    mothur:dist.seqs
    mothur:cluster
    doturlist2matrix_vamps
    otu2tax_vamps
    names2fasta_vamps
    """
    pass
def start_crop(args):
    """
    CROP OTUs
    /groups/vampsweb/seqinfobin/crop/crop.sh -i infile.fa -o crop.otus.fa -z -e -b
    """
    crop_cmd = ['/groups/vampsweb/seqinfobin/crop/crop.sh']
    crop_cmd.append('-i')
    crop_cmd.append(os.getcwd()+'/'+args.infile)
    crop_cmd.append('-o')
    crop_cmd.append(args.infile+'.crop.otus')
    # mk subdir and run it there
    try:
        os.mkdir(os.getcwd()+'/CROP')
        os.chdir(os.getcwd()+'/CROP')
    except:
        print('CROP directory exists')
        sys.exit()
    subprocess.call(crop_cmd)
  
if __name__ == '__main__':
    import argparse
    
    # DEFAULTS
    site = 'vampsdev'
    
    user = ''  
    
    data_object = {}
    
    

    
    myusage = """
   
   otu_uclust_new.py  -i  fasta infile   
   Edit this file to change the cluster size from 97% similarity            
                      
    
    """
    parser = argparse.ArgumentParser(description="Convert any fasta file to uniqued and sorted. Sorts by decreasing abundance." ,usage=myusage)
                                              
    
                                                        
    #parser.add_argument("-site", "--db_host",      required=False,  action="store",   dest = "site", default='vampsdev',
    #                                                help="""database hostname: vamps or vampsdev
    #                                                    [default: vampsdev]""")  
    
    parser.add_argument("-i", "--infile",         required=True,  action="store",   dest = "infile",    help="fasta file")  
                                              
    parser.add_argument("-m", "--otu_method", required=False,  action="store",   dest = "method", default='uclust',
                                                    help="methods: uclust, crop, slp")  
                                                    
    #parser.add_argument("-fa", "--fastafile",  required=True,  action="store",   dest = "in_fastafile", 
    #                                                help="Fasta File ") 
    #parser.add_argument("-uc", "--uclustfile",  required=True,  action="store",   dest = "uclustfile", 
    #                                                help="Fasta File ")                                 
    #parser.add_argument('-s', '--size',         required=True,   action="store",   dest = "size",        
    #                                             help = 'cluster size (0.97)')                                                 
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
    if args.method == 'slp':
        start_slp(args)
    elif args.method == 'crop':
        start_crop(args)
    elif args.method == 'uclust':
        start_uclust(args)
    else:
        print('Could not find this method: '+args.method)
    