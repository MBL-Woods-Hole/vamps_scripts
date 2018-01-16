#!/usr/bin/env python


# -*- coding: utf-8 -*-

# Copyright (C) 2011, Marine Biological Laboratory
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version.
#
# 
#

import os
from stat import * # ST_SIZE etc
import sys
import shutil
import random
import csv
sys.path.append( '/bioware/python/lib/python2.7/site-packages/' )
import MySQLdb
#from apps.ConMySQL import Conn
import datetime
import subprocess

 


def get_metadata(args):
    #with open(args.infile, 'rb') as csvfile:
    print 'opening/reading file'
    data = list(csv.reader(open(args.infile, 'rb'), delimiter='\t'))
    metadata = {}
    datalines_by_ds = {}
    for i,line in enumerate(data):
      if i == 0:
        headers = line[1:]
      else:
        # should be only one line per ds
        # datset name must always be first
        d = line[0]
        datalines_by_ds[d] = line[1:]    
    for ds in datalines_by_ds:
      if ds not in metadata:
        metadata[ds] = {}
      for i,name in enumerate(headers):
        v = datalines_by_ds[ds][i]
        #if name not in metadata[ds]:
        metadata[ds][name]=v
    print 'done generation metadata hash'
    return metadata
     
def load_metadata(args, md):
    print 'loading table'
    if args.site == 'vamps':
        host = 'vampsdb'
    else:
        host = 'vampsdev'
    dbconn = MySQLdb.connect(host=host, # your host, usually localhost
                     user="vamps_w", # your username
                      passwd="g1r55nck", # your password
                      db="vamps") # name of the data base
                      
    cursor = dbconn.cursor()                  
    metadata_table = 'vamps_metadata_MBE'
    alpha = 'Alphanumeric'
    q = "insert ignore into "+metadata_table
    q += " (project,dataset,project_dataset,parameterName,structured_comment_name,parameterValue,units,miens_units,entry_date)"
    q += " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    for ds in md:
      for name in md[ds]:
        print name
        pjds = args.project + '--' + ds
        v = md[ds][name]
        if str(name) == 'latitude' or str(name) == 'longitude':
          scn = name[:3]
        else:
          scn = name
        if str(name) == 'env_feature':
          print 'ENVO:feature: ',v
        #print scn
        vals = (args.project,ds,pjds,name,scn,v,alpha,alpha,args.datetime)
        #print vals
        q1 = q % vals
        print q1
        cursor.execute(q1)
   
    print 'done loading table:',metadata_table
    dbconn.close()
    
if __name__ == '__main__':
    import argparse
    
    # DEFAULTS
    site = 'vampsdev'
    user = ''  

    
    myusage = """usage: load_qiita_metadata.py [options]
         
         Load MoBE metadata into the database
         ie: study_2029_mapping_file.txt
         where
            -i, --infile The name of the input *.txt file.  [required]
            
            -p, --project    The name of the VAMPS project.   [required]
                                   
            -site            vamps or vampsdev.    [default: vampsdev]
            
            -h help
    """
    parser = argparse.ArgumentParser(description="Loads qiime/qiita metadata into database." ,usage=myusage)
    parser.add_argument('-i', '--infile',       required=True, action="store",   dest = "infile", 
                                                    help = 'QIIME/QIITA Metadata file: usually in /templates dir')                                                 
    
    parser.add_argument("-site","--site",         required=False,  action="store",   dest = "site", default='vampsdev',
                                                    choices=['vampsdev','vamps'], help="""""") 
    parser.add_argument("-p", "--project",      required=True,  action="store",   dest = "project", 
                                                    help="VAMPS project name")                                                 
  
    if len(sys.argv[1:]) == 0:
        print myusage
        sys.exit() 
    args = parser.parse_args()
    
    args.datetime = str(datetime.date.today())
    print "SITE: ",args.site
    
    md = get_metadata(args)       
    load_metadata(args, md)
        
    
        
    