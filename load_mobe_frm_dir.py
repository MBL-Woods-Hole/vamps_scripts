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
import pymysql as MySQLdb

import datetime
import subprocess

    

def create_dir_struct(args):

    code = args.user+'_'+str(random.randrange(10000000, 99999999))
    print code
    dir = os.path.join('/groups/vampsweb/'+args.site+'/tmp/',code)
    if not os.path.exists(dir):
        os.makedirs(dir)
        os.makedirs(dir+'/analysis')
        os.makedirs(dir+'/analysis/gast')
    
    return code

def get_datasets(args):
    """
   
    
    """ 
    print args  
    sys.path.append('/groups/vampsweb/'+args.site+'/seqinfobin/merens-illumina-utils/')
    import IlluminaUtils.lib.fastalib as fastalib
    # errors here are between 240 - 249
    seq_allowed = dict.fromkeys('AGCTUNRYMKSWHBVDagctunrymkswhbvd')
    readid_allowed = dict.fromkeys("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:")
    bad_line=False
    dir = os.path.join('/groups/vampsweb/'+args.site+'/tmp/',args.code)
    gast_dir = os.path.join(dir,'analysis/gast')
    datasets = {}
 

    
    
    out_file = os.path.join(dir,"SEQFILE_CLEAN.FA")
    for infile in os.listdir(args.indir):
        #if fileName[-3:]=='.fa':
        dataset = infile[:-3]
        datasets[dataset] = 0
        file_handles = {}
        
        new_dir = os.path.join(gast_dir,dataset)
        print new_dir
        os.makedirs(new_dir)
        # open new fa file
        fasta_file = os.path.join(new_dir,'seqfile.fa')
        fh = open(fasta_file,'w')
        # write defline and seq
        
        file_handles[dataset] = fh
        
        file_path = os.path.join(args.indir,infile)
        
        #if os.path.exists(infile):
        
        #import fastalib
        out_fh = open(out_file,'w')
       
        # if multiple datasets in fa file then must use raw
        # to be able to get ds and id from defline
        # BUT if single have to assume that id is firat and should use single
        raw_id = False
        unique = False
        
        raw_id = True
            
        # should not unique until separated into datasets!!
        f=fastalib.SequenceSource(file_path, unique=unique)    
        # defline could be separated by spaces or '|'
        # from uclust otu creation: >Cluster10108;size=1  breaks here
        counter=0
        
        
        while f.next():
            
           
            id_clean = f.id
            
            
            #print "ID:",id_clean
            if not all(x in seq_allowed for x in f.seq):
                bad_line = True
                msg= 'Sequence failed: '+f.seq
                sys.exit()
            else:
                seq_clean = f.seq.upper().strip()
                
            
            
                       
                #write to fa file
            file_handles[dataset].write('>'+id_clean+'\n'+seq_clean+'\n')
            
#             else:
#                 # create new directory in /gast
#                 
#                 new_dir = os.path.join(gast_dir,dataset)
#                 print new_dir
#                 os.makedirs(new_dir)
#                 # open new fa file
#                 fasta_file = os.path.join(new_dir,'seqfile.fa')
#                 fh = open(fasta_file,'w')
#                 # write defline and seq
#                 fh.write('>'+id_clean+'\n'+seq_clean+'\n')
#                 file_handles[dataset] = fh
#                 # save dataset to datasets
                
            
            if not bad_line:
                out_fh.write('>'+dataset+' '+id_clean+"\n")
                out_fh.write(seq_clean+"\n")                
                counter +=1
        datasets[dataset] = counter
        if bad_line:
            print msg
            sys.exit(241)
        if counter==0:
            print "No sequences found! Remove any empty lines or comments at the top of your file and try again."
            sys.exit(242)
        #print str(counter)+" sequences processed"
        out_fh.close()
    #else:
    #    print "Could not find infile.",file_path
    #    sys.exit(244)
    
    
    sequence_count = counter
    print "sequence_count="+str(sequence_count)
    print 'dir',dir
    print datasets
    return datasets
    
def write_info_file(args):
    """

    """
    project_count = sum(args.datasets.values())
    print 'pcount',project_count
    info_filename = 'INFO-LOAD.config'
    dir = os.path.join('/groups/vampsweb/'+args.site+'/tmp/',args.code)
    info_file = os.path.join(dir, info_filename)
    fh = open(info_file,'w')
                
    fh.write(';COMMENT'+'\n\n')
    fh.write('[MAIN]'+'\n')
    fh.write('classifier='+'\n')
    fh.write('upload_type=pretrimmed'+'\n')
    fh.write('project='+args.project+'\n')
    fh.write('date='+args.datetime+'\n')
    fh.write('file_base='+dir+'\n')
    fh.write('chimera_check=no'+'\n')
    fh.write('directory='+args.code+'\n')
    fh.write('has_tax=0'+'\n')
    fh.write('project_sequence_count='+str(project_count)+'\n')
    fh.write('sequence_counts_are=NOT_UNIQUE'+'\n')
    fh.write('number_of_datasets='+str(len(args.datasets))+'\n')
    fh.write('vamps_user='+args.user+'\n')
    fh.write('dna_region='+args.dna_region+'\n')
    fh.write('domain='+args.domain+'\n')
    fh.write('env_source_id=100'+'\n')
    fh.write('public=no'+'\n\n')
    
    fh.write('[DATASETS]'+'\n')
    for ds in args.datasets:
      fh.write(ds+'='+str(args.datasets[ds])+'\n')
    fh.close()
    
def write_info_sql_tables(args):
    """
  
    """
    if args.site == 'vamps':
        host = 'vampsdb'
    else:
        host = 'vampsdev'
    project_count = sum(args.datasets.values())
    dbconn = MySQLdb.connect(host=host, # your host, usually localhost
                     user="vamps_w", # your username
                      passwd="g1r55nck", # your password
                      db="vamps") # name of the data base
                      
    cursor = dbconn.cursor()                  
    info_table     = 'vamps_upload_info'
    datasets_table = 'vamps_projects_datasets_pipe'
    status_table   = 'vamps_upload_status'
    
    # vamps_upload_info
    q_info = "insert into "+info_table
    q_info += " (project_name,contact,email,institution,user,env_source_id,upload_date,upload_function,has_tax,seq_count,project_source,public)"
    q_info += " VALUES(%s,'Andrew Voorhis','avoorhis@mbl.edu','MBL',%s,'100',%s,'pretrimmed','0',%s,'user_upload','no')"  # 4 variables
    vals = (args.project,args.user,args.datetime,str(project_count))
    cursor.execute(q_info,vals)
    
    # vamps_projects_datasets_pipe
    q_pjds = "insert ignore into "+datasets_table
    q_pjds += " (project,dataset,dataset_count,has_tax,date_trimmed,dataset_info)"
    q_pjds += " VALUES(%s,%s,%s,'0',%s,%s)"  # 5 variables
    for ds in args.datasets:
        vals = (args.project,ds,str(args.datasets[ds]),args.datetime,ds)
        cursor.execute(q_pjds,vals)
    
    # vamps_upload_status
    id = args.code.split('_')[1]
    q_status = "insert into "+status_table
    q_status += " (id,user,date,status,status_message)"
    q_status += " VALUES(%s,%s,%s,'LOAD_SUCCESS','Your VAMPS sequence loading is complete')"  # 3 variables
    vals = (id, args.user,args.datetime)
    cursor.execute(q_status,vals)
    
    dbconn.close()
    
if __name__ == '__main__':
    import argparse
    
    # DEFAULTS
    site = 'vampsdev'
    user = 'admin'  
    
    
    myusage = """usage: vamps_load_frm_dir.py [options]
         
                 
         where
            ###-i, --infile The name of the input fasta file.  [required]
            -dir  directory where fa files reside
            -site, --site            vamps or vampsdev.
                                [default: vampsdev]
            -u, --user          [default: admin]
             
            -p, --project    The name of the project.   [required]
            
            -dna_region       [default: unknown]
            
            -domain       [default: unknown]
            
            -h help
    
    """
    parser = argparse.ArgumentParser(description="Loads MoBE sequence data into vampsweb tmp directory" ,usage=myusage)
    #parser.add_argument('-i', '--infile',       required=True, action="store",   dest = "infile", 
    #                                                help = '')                                                                                 
    parser.add_argument('-dir', '--directory',  required=True, action="store",   dest = "indir", default='',
                                                    help = '')    
    parser.add_argument("-u", "--user",         required=False,  action="store",   dest = "user", default='admin',
                                                    help="VAMPS user name")  
                                                     
    parser.add_argument("-site","--site",         required=False,  action="store",   dest = "site", default='vampsdev',
                                                    choices=['vampsdev','vamps'], help="""""") 
    parser.add_argument("-p", "--project",      required=True,  action="store",   dest = "project", 
                                                    help="VAMPS project name")      
    parser.add_argument("-dna_region",       required=False,  action="store",   dest = "dna_region", default='unknown',
                                                    help="eg: v6, v3v5, ITS, unknown") 
    parser.add_argument("-domain","--domain",       required=False,  action="store",   dest = "domain", default='unknown',
                                                    help="bacteria, archaea, eukarya, fungal, unknown")                                                 
   
    if len(sys.argv[1:]) == 0:
        print myusage
        sys.exit()
    args = parser.parse_args()
    
    args.datetime = str(datetime.date.today())    
    args.project = args.project[:1].capitalize()+args.project[1:]
    
    args.code = create_dir_struct(args)
    args.datasets = get_datasets(args)
    write_info_file(args)
    write_info_sql_tables(args)   # write to vamps_upload_info and vamps_projects_datasets_pipe and vamps_status_update
            
      