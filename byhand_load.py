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
sys.path.append( '/bioware/python/lib/python2.7/site-packages/' )

import datetime
import subprocess
#sys.path.append("/bioware/merens-illumina-utils/")
#import IlluminaUtils.lib.fastqlib as fastqlib
#from vamps_utils import FastaReader

 
            

def go_multi(args):
    """
    NO:need qiime map file for ds names only
    and fasta file with defline like so:   >ds|id|frequency:23
    Should create directory structure: analysis/gast/ds for each ds found in seqfile
    
    
    """
    import IlluminaUtils.lib.fastalib as fastalib
    infile  = args.infile
    unique = False
    data = {}
    cnt = '1'
    f=fastalib.SequenceSource(infile, unique=unique)
    while f.next():
        defline_items = f.id.split(args.delim)
        dataset = defline_items[0]
        # if ds like M9Dkey217.141053_69
        # must remove the _69 from end
        # but not if like M9Dkey217_141053
        # so:
        test_ds_parts = dataset.split('_')
        if RepresentsInt(test_ds_parts[-1]):
            dataset = '_'.join(test_ds_parts[:-1])
        id = defline_items[1].split()[0] # M01028:102:000000000-AK07B:1:1101:19698:4186 1:N:0:6
        freq = defline_items[-1]
        if dataset not in data:
            data[dataset] = []
            
        if freq[:4] == 'freq':
            try:
                cnt = freq.split(':')[1]
            except:
                try:
                    cnt = freq.split('=')[1]
                except:
                    cnt = '1'
        data[dataset].append({'id':id, 'seq':f.seq, 'cnt':cnt})
        
        analysis_dir = 'analysis'
        gast_dir     = 'analysis/gast'
        
        if not os.path.exists(analysis_dir):
            os.makedirs(analysis_dir)
        if not os.path.exists(gast_dir):
            os.makedirs(gast_dir)    
        for ds in data:
            
            if ds != '':
                dir = os.path.join(gast_dir,ds)
                if os.path.exists(dir):
                    shutil.rmtree(dir)
                os.makedirs(dir)
                
                outfile = os.path.join(dir,args.outfile)
                fh = open(outfile,'w')
                for dict in data[ds]:
                    
                    cnt = dict['cnt']
                    id  = dict['id']
                    seq = dict['seq']
                    if RepresentsInt(cnt):      
                        for m in range(1,int(cnt)+1):                    
                            idcnt = id + '_' + str(m)
                            if args.stdout:
                                print '>'+idcnt+'\n'+seq
                            else:
                                fh.write('>'+idcnt+'\n'+seq+'\n')
                    else:
                        if args.stdout:
                            print '>'+id+'\n'+seq
                        else:
                            fh.write('>'+id+'\n'+seq+'\n')
                fh.close()
                
            else:
                print 'Empty ds name!!'
        
        

def go_single(args):
    """
        reads input fa file
        finds frequencies if present and expands 
        writes out  SEQFILE_CLEAN.fa in same directory
    """
    
    #sys.path.append('/groups/vampsweb/'+args.site+'/seqinfobin/merens-illumina-utils/')
    import IlluminaUtils.lib.fastalib as fastalib
    infile  = args.infile
    
    
    print args.infile
    unique = False
    # should not unique until separated into datasets!!
    f=fastalib.SequenceSource(infile, unique=unique)
    pcounter=0
        
    datasets = {}
    file_handles = {}
    fh = open(args.outfile,'w')
    cnt = '1'
    while f.next():
        
        defline_items = f.id.split('|')
        id_clean = defline_items[0].split()[0]                
        freq = defline_items[-1]
        seq_clean = f.seq.upper().strip()
        #print freq
        
        if freq[:4] == 'freq':
            try:
                cnt = freq.split(':')[1]
            except:
                try:
                    cnt = freq.split('=')[1]
                except:
                    cnt = '1'
        if RepresentsInt(cnt):      
            for i in range(1,int(cnt)+1):                    
                id = id_clean + '_' + str(i)
                if args.stdout:
                    print '>'+id+'\n'+seq_clean
                else:
                    fh.write('>'+id+'\n'+seq_clean+'\n')
                
        else:
            if args.stdout:
                print '>'+id_clean+'\n'+seq_clean
            else:
                fh.write('>'+id_clean+'\n'+seq_clean+'\n')
                
        
    fh.close()
    
   

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
 


if __name__ == '__main__':
    import argparse
    
   
    
    myusage = """usage: byhand_load.py [options]
         
         Load user sequences into the database
         
         where
            -i, --infile The name of the input file file.  [required]
            -delim  delimiter for multi dataset option between dataset and id default is [space]
            -fa_style multi or single -- are different formats of fasta files
                    multi: >ds<space>id<space>other stuff
            -stdout 
            
            
            
    
    
    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)
    parser.add_argument('-i', '--infile',       required=True, action="store",   dest = "infile",  help = '')  
    parser.add_argument('-o', '--outfile',      required=False, action="store",   dest = "outfile", default='SEQFILE_CLEAN.fa',  help = '')                                               
  #   parser.add_argument("-t", "--upload_type",         required=True,  action="store",   dest = "type", 
#                                                     help="raw or trimmed or metadata")                                   
#     
#     
#                                                      
#     parser.add_argument("-site",                required=True,  action="store",   dest = "site", 
#                                                     help="""""")  
#     parser.add_argument("-r", "--runcode",      required=False,  action="store",   dest = "runcode", 
#                                                     help="""""")  
#  
#     parser.add_argument("-u", "--user",         required=True,  action="store",   dest = "user", 
#                                                     help="user name")  
     
    parser.add_argument('-delim','--delim',       required=False, action="store",   dest = "delim", default= ' ',
                                                     help = '')                                                   
#                                                     
# ## optional       
#     parser.add_argument("-dna_region",       required=False,  action="store",   dest = "dna_region", default='unknown',
#                                                     help="") 
#     parser.add_argument("-domain",       required=False,  action="store",   dest = "domain", default='unknown',
#                                                     help="")                                                 
#     parser.add_argument('-d', '--dataset',      required=False, action="store",   dest = "dataset",  
#                                                     help = '')                                                 
#     parser.add_argument("-p", "--project",      required=False,  action="store",   dest = "project", 
#                                                     help="")                                                   
#     parser.add_argument("-reuse", "--reuse",      required=False,  action="store_true",   dest = "reuse_project", default=False,
#                                                     help="")                                                            
    parser.add_argument('-fa_style','--fa_style',   required=False, action="store",   dest='fasta_style', help = 'multi or single', default='single_ds')                                                    
    parser.add_argument('-stdout','--stdout',   required=False, action="store_true",   dest='stdout', help = 'true or false', default=False)
   
    args = parser.parse_args()
    
    args.datetime = str(datetime.date.today())
    
    if args.fasta_style == 'multi':
        go_multi(args)
    else:
        go_single(args)
        
        
        
   