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

all_user_list = ['avoorhis','andy','mitchell','eren','anya','rob','joshua','yoshiki','adam','antonio','paula',
                 'xuanhe89','mechah','paragvai','ksteen','mnguyen','pinderb','kmkalanetra','isitepu',
                 'cfrench','clowe','mmoore','klbmills','zlewis','radams','mhernandez','idrygiannakis',
                 'simonlax','jcalderon','hcavallin','tmayer'];

admin_user_list = ['avoorhis','andy','mitchell','eren','anya','rob','joshua','yoshiki','adam','antonio','paula'];

    
def add_users(args):
    """
  
    """
    if args.site == 'vamps':
        host = 'vampsdb'
    else:
        host = 'vampsdev'
    
    dbconn = MySQLdb.connect(host=host, # your host, usually localhost
                     user="vamps_w", # your username
                      passwd="g1r55nck", # your password
                      db="vamps") # name of the data base
                      
    cursor = dbconn.cursor()                  
    user_table     = 'vamps_users'
    
    # vamps_users
    # insert into vamps_users (user,project) VALUES ('','')
    
    q_info = "insert ignore into "+user_table
    q_info += " (user,project)"
    q_info += " VALUES('%s','"+args.project+"')" 
    if args.privacy == 'single':
      query = q_info % (args.user)
      print query
      #print args.enter
      if args.enter:
        cursor.execute(query)
        pass
      else:        
        print "TEST Only: add -enter to command line to write to DB on "+args.site
    elif args.privacy == 'all':
      list = all_user_list
    else:
      list = admin_user_list
      for name in list:
    
        query = q_info % (name)
        print query
        #print args.enter
        if args.enter:
          cursor.execute(query)
          pass
        else:        
          print "TEST Only: add -enter to command line to write to DB on "+args.site
    
    
    
    dbconn.close()
    

         
if __name__ == '__main__':
    import argparse
    
    # DEFAULTS
    site = 'vampsdev'
    user = 'admin'  
    
    
    myusage = """usage: add_users2project.py [options]
         
                 
         where           
            
            -site, --site            vamps or vampsdev.
                                [default: vampsdev]
                        
            -p, --project    The name of the project.   [required]
            
            -priv: all, admin or single: will add users to vamps_users for this project
                                 -admin: 'andy','mitchell','eren','anya','rob','joshua','yoshiki','adam','antonio','paula'
                                 -all: includes all students as well (not that useful)
                                 -single: must enter username
            -u, --user    single username to attach to project if priv==single
            -e  will allow db to be written
            -h help
            
    
    """
    parser = argparse.ArgumentParser(description="puts user name(s) into vamps_user " ,usage=myusage)
   
                                                     
    parser.add_argument("-site","--site",         required=False,  action="store",   dest = "site", default='vampsdev',
                                                    choices=['vampsdev','vamps'], help="""""") 
    parser.add_argument("-p", "--project",      required=True,  action="store",   dest = "project", 
                                                    help="VAMPS project name")      
    parser.add_argument("-e", "--enter",      action="store_true",   dest = "enter", 
                                                    help="required to enter datain DB")                                        
    parser.add_argument("-priv",      required=True, action="store",   dest = "privacy", choices=['admin','all','single'],
                                                    help="For MoBE Workshop Jan 2015 in Davis CA")
    parser.add_argument("-u","--user",      required=False, action="store",   dest = "user", default='',
                                                    help="")                                                
    args = parser.parse_args()
    if args.privacy == 'single' and not args.user:
       print "privacy=single but no user entered"
       sys.exit()
    add_users(args)
         
      
