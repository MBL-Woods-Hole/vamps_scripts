#!/usr/bin/env python

""" 
  clean_vamps_files.py


"""

import sys, os, io
import argparse

try:
    import mysqlclient as mysql
except ImportError:
    try:
        import pymysql as mysql
    except ImportError:
        import MySQLdb as mysql
import json
import shutil
import datetime
import socket
from collections import defaultdict
import time



def it_is_py3():
    if sys.version_info[0] < 3:
        return False
    if sys.version_info[0] >= 3:
        return True


class MyConnection:
    """
    Takes parameters from ~/.my.cnf, default host = "vampsdev", db="test"
    if different use my_conn = MyConnection(host, db)
    """

    def __init__(self, host = "bpcweb7", db = "test", read_default_file = ""):
        # , read_default_file=os.path.expanduser("~/.my.cnf"), port = 3306

        self.conn = None
        self.cursor = None
        self.cursorD = None
        self.rows = 0
        self.new_id = None
        self.lastrowid = None

        port_env = 3306
        try:
            print("host = " + str(host) + ", db = " + str(db))
            print("=" * 40)
            read_default_file = os.path.expanduser("~/.my.cnf_node")

            if is_local():
                host = "127.0.0.1"
                read_default_file = "~/.my.cnf_local"
            self.conn = mysql.connect(host = host, db = db, read_default_file = read_default_file, port = port_env)
            self.cursor = self.conn.cursor()
            self.cursorD = self.conn.cursor(mysql.cursors.DictCursor)

        except (AttributeError, mysql.OperationalError):
            self.conn = mysql.connect(host = host, db = db, read_default_file = read_default_file, port = port_env)
            self.cursor = self.conn.cursor()
        except mysql.Error:
            e = sys.exc_info()[1]
            print("Error %d: %s" % (e.args[0], e.args[1]))
            raise
        except:  # catch everything
            print("Unexpected:")
            print(sys.exc_info()[0])
            raise  # re-throw caught exception

    @staticmethod
    def connect(host, db, read_default_file, port_env):
        return mysql.connect(host = host, db = db, read_default_file = read_default_file, port = port_env)

    def execute_fetch_select(self, sql):
        if self.cursor:
            try:
                self.cursor.execute(sql)
                return self.cursor.fetchall()
            except:
                print("ERROR: query = %s" % sql)
                raise

    def execute_no_fetch(self, sql):
        if self.cursor:
            self.cursor.execute(sql)
            self.conn.commit()
            try:
                return self.cursor._result.message
            except:
                return self.cursor._info

    def execute_fetch_select_dict(self, sql):
        if self.cursorD:
            try:
                self.cursorD.execute(sql)
                return self.cursorD.fetchall()
            except:
                print("ERROR: query = %s" % sql)
                raise


def is_local():
    print(os.uname()[1])
    dev_comps = ['ashipunova.mbl.edu', "as-macbook.home", "as-macbook.local", "Ashipunova.local",
                 "Annas-MacBook-new.local", "Annas-MacBook.local"]
    if os.uname()[1] in dev_comps:
        return True
    else:
        return False

today = str(datetime.date.today())

did_query = "SELECT dataset_id from dataset order by dataset_id"


def make_file_paths(args):
    file_paths = {}
    file_paths['did_paths'] = []
    file_paths['did_paths'].append(os.path.join(args.json_file_path, args.NODE_DATABASE + '--datasets_silva119'))
    file_paths['did_paths'].append(os.path.join(args.json_file_path, args.NODE_DATABASE + '--datasets_rdp2.6'))
    file_paths['did_paths'].append(os.path.join(args.json_file_path, args.NODE_DATABASE + '--datasets_generic'))
    file_paths['metadata'] =       os.path.join(args.json_file_path, args.NODE_DATABASE + '--metadata.json')
    return file_paths


def get_all_dids(args):
    rows = myconn.execute_fetch_select(did_query)
    all_db_dids = []
    for row in rows:
        all_db_dids.append(str(row[0]))
    #print(all_db_dids)
    return all_db_dids



def go_add(args, fpaths, all_dids):
    # we have all dids
    print('\n'+'-'*40)
    # to scan each ds directory
    print('Running INDIVIDUAL JSON FILES:')
    process_individual_json_files(args, fpaths['did_paths'])
    print('\n'+'-'*40)
    # open bulk metadata file and parse good ones into new object
    print('Running BULK METADATA FILE:')
    process_bulk_metadata_file(args, fpaths['metadata'])
    
def process_individual_json_files(args, fpaths):
    for fpath in fpaths:
        print('Directory: '+fpath)
        orphan_count=0
        for f in os.listdir(fpath):
            did = str(f.split('.')[0])
            
            if did not in all_dids:
                # We have found an orphan did
                file_to_delete = os.path.join(fpath, did+'.json')                
                if args.delete_dids:
                    print('deleting orphan: '+file_to_delete)
                    os.remove(file_to_delete)
                else:
                    #print('No deletion of orphan did file: per deletions CL flag '+did)  
                    pass                  
                orphan_count +=1
            else:
                #print('GOOD'+did)
                pass
        if args.delete_dids:
            if orphan_count == 0:
                print('Found: '+str(orphan_count)+' Orphan file(s)')
            else:
                print('Found: '+str(orphan_count)+' Orphan file(s) -- DELETED') 
        else:
            if orphan_count == 0:
                print('Found: '+str(orphan_count)+' Orphan file(s)')
            else:
                print('Found: '+str(orphan_count)+' Orphan file(s) -- Not Deleted')  
                
def process_bulk_metadata_file(args, md_file):    
    
    if args.delete_dids and not args.no_backup:
        from random import randrange
        rando = randrange(10000, 99999)
        bu_file = os.path.join(args.json_file_path, args.NODE_DATABASE + "--metadata_" + today + '_' + str(rando) + ".json")
        print('Backing up bulk metadata file to', bu_file)
        try:
        	shutil.copy(md_file, bu_file)
        except:
        	print('NO BACKUP Of Metadata File: FILE NOT FOUND')
    
    new_metadata_object = {}    	
    with open(md_file) as json_file:
        data = json.load(json_file)
        orphan_count=0
        db_count = 0
        for did in data:
            #print(did)
            if did not in all_dids:
                # We have found an orphan did
                #print('MD-Orphan: '+did)
                orphan_count +=1
            else:
                # Add good one to new Object
                new_metadata_object[did] = data[did]
                db_count += 1
        if args.delete_dids:
            if orphan_count == 0:
                print('Found: '+str(orphan_count)+' Orphan file(s)')
            else:
                print('Found: '+str(orphan_count)+' Orphan file(s) -- DELETED')            
        else:
            if orphan_count == 0:
                print('Found: '+str(orphan_count)+' Orphan file(s)')
            else:
                print('Found: '+str(orphan_count)+' Orphan file(s) -- Not Deleted')  
        print('Found: '+str(db_count)+' Good Dataset IDs(s)')
    
    # we've backed up the original so no we want to overwrite the original name
    if len(new_metadata_object.keys()) == len(data.keys()):
        print('No Bulk File Deletions')
    
    if args.delete_dids:
        with open(md_file, 'w') as outfile:  # overwrite or create new
            json.dump(new_metadata_object, outfile)
    else:
        print('No change to bulk metadata file: per delete_dids CL flag')
        


if __name__ == '__main__':
    start0 = time.time()

    myusage = """
    
    This script's purpose is to check and remove orphan dataset_ids from the vamps-node.js JSON files. 
    
        Required:
        -host                   vampsdb, vampsdev, localhost    dbhost:  [Default: localhost]
        
        Optional:
        
        -jfp/--json_file_path   json files path [Default: ./json]
        
        -del/--delete_dids      When used will delete Dataset Files and modify Bulk Metadata File
                                [Default is to NOT delete] 
        


    """

    parser = argparse.ArgumentParser(description = "", usage = myusage)
    #group = parser.add_mutually_exclusive_group(required=True)
    
    
    parser.add_argument("-no_backup", "--no_backup",
                        required = False, action = "store_true", dest = "no_backup", default = False,
                        help = """no_backup of group files: taxcounts and metadata""")
    parser.add_argument("-metadata_warning_only", "--metadata_warning_only",
                        required = False, action = "store_true", dest = "metadata_warning_only", default = False,
                        help = """warns of datasets with no metadata""")
    parser.add_argument("-jfp", "--json_file_path",
                        required = False, action = 'store', dest = "json_file_path", default = './json',
                        help = "Not usually needed if -host is accurate")
    
    parser.add_argument("-host", "--host",
                        required = True, action = 'store', dest = "dbhost", default = 'localhost',
                        help = "choices=['vampsdb', 'vampsdev', 'vampscloud', 'localhost']")
    parser.add_argument("-del", "--delete_dids",
                        required = False, action = "store_true", dest = "delete_dids", default = False,
                        help = """no_deletions : query the files only""")
    


    # if len(sys.argv[1:]) == 0:
    #     print(myusage)
    #     sys.exit()
    args = parser.parse_args()
    print(args)
    annas_local_hosts = ['Annas-MacBook.local', 'Annas-MacBook-new.local', 'AnnasMacBook.local']
    is_annas_localhost = socket.gethostname() in annas_local_hosts
    
    if args.dbhost == 'vamps' or args.dbhost == 'vampsdb' or args.dbhost == 'bpcweb8':
        args.json_file_path = '/groups/vampsweb/vamps/nodejs/json'
        dbhost = 'vampsdb'
        args.NODE_DATABASE = 'vamps2'

    elif args.dbhost == 'vampsdev' or args.dbhost == 'bpcweb7':
        args.json_file_path = '/groups/vampsweb/vampsdev/nodejs/json'
        args.NODE_DATABASE = 'vamps2'
        dbhost = 'bpcweb7'
    elif args.dbhost == 'vampscloud':
        args.json_file_path = '/vol_b/vamps/json'
        args.NODE_DATABASE = 'vamps_development'
        dbhost = 'localhost'
    elif args.dbhost == 'localhost' and is_annas_localhost:
        args.NODE_DATABASE = 'vamps2'
        dbhost = 'localhost'
    else:
        dbhost = 'localhost'
        args.NODE_DATABASE = 'vamps_development'
  
    print("\nARGS: dbhost  =", dbhost)
    print("\nARGS: NODE_DATABASE  =", args.NODE_DATABASE)
    print("ARGS: json_file_path =", args.json_file_path)
    
    #1 make paths from json_files_dir
    fpaths = make_file_paths(args)
    
    if os.path.exists(args.json_file_path) and os.path.isfile(fpaths['metadata']):
        print('** Validated json_file_path **')
    else:
        print(myusage)
        print("Could not find json directory: '"+args.json_file_path+ "/' Or Bulk metadata file --Exiting\n")
        sys.exit(-1)
    database = args.NODE_DATABASE
    myconn = MyConnection(dbhost, database, read_default_file = "~/.my.cnf_node")

    if not args.NODE_DATABASE:
        databases = myconn.execute_fetch_select("SHOW databases like 'vamps%'")
        args.NODE_DATABASE = ask_current_database(databases)

    # out_file = "tax_counts--"+NODE_DATABASE+".json"
    # in_file  = "../json/tax_counts--"+NODE_DATABASE+".json"

    print('DATABASE:', args.NODE_DATABASE)
    all_dids = get_all_dids(args)
    
    
    go_add(args, fpaths, all_dids)
    if not args.delete_dids:
        print("\n*** No Changes Occured:: to delete dids add '-del' to command line. ***\n")
    else:
        print('\nDone\n')

