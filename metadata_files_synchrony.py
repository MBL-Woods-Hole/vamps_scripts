#!/usr/bin/env python

"""
  create_counts_lookup.py


"""

import sys,os,io
import argparse
try:
    import pymysql as mysql
except ImportError:
    import MySQLdb as mysql
except:
    raise
import json
import shutil
import datetime
import socket
from collections import defaultdict

today     = str(datetime.date.today())


"""

"""


parser = argparse.ArgumentParser(description="")


def convert_keys_to_string(dictionary):
    """Recursively converts dictionary keys to strings."""
    if not isinstance(dictionary, dict):
        return dictionary
    return dict((str(k), convert_keys_to_string(v))
        for k, v in dictionary.items())

def go_list(args):

    #
    file_dids = []

    metadata_lookup = convert_keys_to_string(read_original_metadata())
    (projects_by_did, project_id_lookup, project_lookup) = get_project_lookup(args) # from database
    #print(project_lookup)
    required_metadata_fields = get_required_metadata_fields(args)
    #print('file_dids')
    #print(metadata_lookup)  ## <-- lookup by did
    
    project_id_order = list(project_id_lookup.keys())
    project_id_order.sort()
    #metadata_dids = metadata_lookup.keys()
    #
   
    failed_projects = []
    no_req_data_found = 0
    no_file_found = {}          # 2only for metadata BULK FILE: if did NOT found
    mismatch_data = {}          # 1metadata mismatch between DATABASE and metadata BULK FILE
    cust_rowcount_data = {}     # 4dataset count unequal between DATABASE custom metadata table and DATABASE datasets table
    other_problem = {}
    did_file_problem = {}       # 5did FILES: if taxcounts ={} or empty file present
    did_file_problem_by_pid = defaultdict(list)
    no_req_metadata = {}        # 3if no required metadata found in DATABASE
    num_cust_rows = 0 
    
    if args.single_pid:
        temp_id_lookup = {}
        temp_id_lookup[args.single_pid] = project_id_lookup[args.single_pid]
        project_id_order = [args.single_pid]
        project_id_lookup = temp_id_lookup
        print('Searching Single PID:',args.single_pid)
        print(project_id_lookup)
        ds_count = len(project_id_lookup[args.single_pid]) 
        print("ds_count from DB:",ds_count)
        
    for pid in project_id_order:
        # go project by project
        sql_dids =  "','".join(project_id_lookup[pid])
        ds_count = len(project_id_lookup[pid]) 
        q = "SELECT dataset_id, "+ ', '.join(required_metadata_fields) +" from required_metadata_info" 
        q += " WHERE dataset_id in ('%s')" % (sql_dids)
        if args.verbose:
            print(q)
        clean_project = True
        num = 0
        items_before_required_metadata_fields = 1
        cur.execute(q)
        numrows = cur.rowcount
        if numrows == 0:
            no_req_data_found += 1
            #print(str(no_req_data_found)+') No Required metadata for project: '+str(project_lookup[pid])+' ('+str(pid)+')')
            if pid not in no_req_metadata:
                    no_req_metadata[pid] = project_lookup[pid]["p"]
            continue

        for row in cur.fetchall():
            did = str(row[0])
            matrix = str(row[1])
            did_file1 = os.path.join(args.json_file_path, NODE_DATABASE+'--datasets_silva119', str(did)+'.json')
            did_file2 = os.path.join(args.json_file_path, NODE_DATABASE+'--datasets_rdp2.6"',  str(did)+'.json')
            did_file3 = os.path.join(args.json_file_path, NODE_DATABASE+'--datasets_generic',  str(did)+'.json')
            
            #if did == '3938':
            if did not in metadata_lookup:
                if pid not in no_file_found:
                    no_file_found[pid] = project_lookup[pid]["p"]
                clean_project = False
            else:
                for i,item in enumerate(required_metadata_fields):
                   
                    if item in metadata_lookup[did]:
                        db_val = str(row[i+items_before_required_metadata_fields])
                        if str(metadata_lookup[did][item]) != db_val :
                            if args.verbose:
                                print('pid:'+str(pid) +' -- '+project_lookup[pid]["p"]+' -- did:' +did+'  no match-1 for `'+item+'`:',metadata_lookup[did][item],' - ',db_val)
                            if pid not in mismatch_data:
                                mismatch_data[pid] = project_lookup[pid]["p"]
                            clean_project = False
                    else:
                         if args.verbose:
                            print('pid:'+str(pid) +' -- '+'--'+project_lookup[pid]["p"]+' -- did:' +did+' -- `'+item+'` item not found in req metadata file-1\n')
                         if pid not in other_problem:
                            other_problem[pid] = project_lookup[pid]["p"]
                         clean_project = False
            
            try:
                #did_file = os.path.join(args.json_file_path, NODE_DATABASE+'--datasets_silva119', str(did)+'.json')
                fp = open(did_file1)
                file_data = json.load(fp)
                if not file_data or not file_data['taxcounts']:
                    did_file_problem[pid] = project_lookup[pid]["p"]
                    did_file_problem_by_pid[str(pid)].append(str(did))
                fp.close()
            except:
                did_file_problem[pid] = project_lookup[pid]["p"]
                if args.verbose:
                    print('pid:'+str(pid) +' -- '+'--'+project_lookup[pid]["p"]+' -- did:' +did+' -- Could not open did file-1\n')
        
        
        
        custom_metadata_file = 'custom_metadata_'+str(pid)
        
        q1 = "SHOW fields from "+custom_metadata_file
        q2 = "SELECT * from "+custom_metadata_file

        fields = []
        try:
            cur.execute(q1)
            rows = cur.fetchall()
            for row in rows:
                
                field = str(row[0])
                if field !=  custom_metadata_file+'_id' and field != 'dataset_id' and field != 'project_id':
                    fields.append(field) # starts with idx 2
            
            if args.verbose:
                print(q2)
            # query2 q2 = "SELECT * from "+custom_metadata_file
            cur.execute(q2)
            num_cust_rows = cur.rowcount
            if num_cust_rows != ds_count:
                cust_rowcount_data[pid] = project_lookup[pid]["p"]
            
            rows = cur.fetchall()
            for row in rows:
                did = str(row[1]) # first is custom_metadata_<pid>_id, second is did
                
                for i,item in enumerate(fields):

                    #print('pid:',pid,'did:',did, 'item:',item, metadata_lookup[did], project_lookup[pid]["p"])
                    if item in metadata_lookup[did]:
                        #print(item,i,'row',row)
                        db_val = str(row[i+2])

                        if str(metadata_lookup[did][item]) != db_val:
                            if args.verbose:
                                print('pid:'+str(pid) +' -- '+project_lookup[pid]["p"]+' -- did:' +did+'  no match-2 for `'+item+'`:',metadata_lookup[did][item],'!=',db_val)
                            if pid not in mismatch_data:
                                mismatch_data[pid] = project_lookup[pid]["p"]
                            clean_project = False
                    else:
                        if args.verbose:
                            print( 'pid:'+str(pid) +' -- '+project_lookup[pid]["p"]+' -- did:' +did+' -- `'+item+'` item not found in cust metadata file-2\n')
                        if pid not in other_problem:
                            other_problem[pid] = project_lookup[pid]["p"]
                        clean_project = False
        except:
            pass
        #sys.exit()
        #if not clean_project:
        #      failed_projects.append('pid:'+str(pid)+' -- '+project_lookup[pid]["p"])
    missing_seqs = {}
    q2_base = "SELECT sequence_pdr_info_id from sequence_pdr_info where dataset_id in ('%s')"
    if args.verbose:
        print(q2_base)
    if args.search_seqs:
        print('Searching for sequences in sequence_pdr_info')        
        for pid in project_id_order:
            # go project by project
            sql_dids =  "','".join(project_id_lookup[pid])
            ds_count = len(project_id_lookup[pid]) 
            q2 = q2_base % (sql_dids)                
            clean_project = True
            num = 0
            cur.execute(q2)
            numrows = cur.rowcount
            if numrows == 0:
                missing_seqs[pid] = project_lookup[pid]["p"]
                if args.verbose:
                    print('No sequences found::pid:'+str(pid) +' -- '+project_lookup[pid]["p"])
        
    print()
    print('*'*60)
    print('Failed projects:')
    
    print()
    print('\t1) METADATA MIS-MATCHES BETWEEN BULK FILE AND DBASE (Assumes same for did file) (re-build should work):')
    if not len(mismatch_data):
        print('\t **Clean**')
    else:
        for pid in mismatch_data:
            if project_lookup[pid]["perm"] == '0':   # no temporary projects
                pass
            elif project_lookup[pid]["mtx"] == '1':
                print('\t pid:',pid,' -- ',mismatch_data[pid]+" (matrix)")
            else:            
                print('\t pid:',pid,' -- ',mismatch_data[pid])
        print('\t PID List:',','.join([str(n) for n in mismatch_data.keys()]))
    
    print()
    print('\t2) NO DID FOUND IN METADATA BULK FILE (Assumes no did file found either) (re-build should work):')
    if not len(no_file_found):
        print('\t **Clean**')
    else:
        for pid in no_file_found:
            if project_lookup[pid]["perm"] == '0':   # no temporary projects
                pass
            elif project_lookup[pid]["mtx"] == '1': # show matrix status
                print('\t pid:',pid,' -- ',no_file_found[pid]+" (matrix)")
            else:
                print('\t pid:',pid,' -- ',no_file_found[pid])
        print('\t PID List:',','.join([str(n) for n in no_file_found.keys()]))
    
    print()
    print('\t3) NO REQUIRED METADATA FOUND IN DATABASE (re-install project or add by hand -- re-build won\'t help):')
    if not len(no_req_metadata):
        print('\t **Clean**')
    else:
        for pid in no_req_metadata:
            if project_lookup[pid]["perm"] == '0':   # no temporary projects
                pass
            elif project_lookup[pid]["mtx"] == '1': # show matrix status
                print('\t pid:',pid,' -- ',no_req_metadata[pid]+" (matrix)")
            else:
                print('\t pid:',pid,' -- ',no_req_metadata[pid])
        print('\t PID List:',','.join([str(n) for n in no_req_metadata.keys()]))
    
    print()
    print('\t4) DATABASE: Dataset count is different between `dataset` and `custom_metadata_xxx` tables (re-build won\'t help):')
    if not len(cust_rowcount_data):
        print('\t **Clean**')
    else:
        for pid in cust_rowcount_data:
            if project_lookup[pid]["perm"] == '0':   # no temporary projects
                pass
            elif project_lookup[pid]["mtx"] == '1': # show matrix status
                print('\t pid:',pid,' -- ',cust_rowcount_data[pid]+" (matrix)")
            else:
                print('\t pid:',pid,' -- ',cust_rowcount_data[pid])
        print('\t PID List:',','.join([str(n) for n in cust_rowcount_data.keys()]))
    
    print()
    print('\t5) DID FILES: zero-length file or taxcounts={}:')
    if not len(did_file_problem):
        print('\t **Clean**')
    else:
        for pid in did_file_problem:
            if project_lookup[pid]["perm"] == '0':   # no temporary projects
                pass
            elif project_lookup[pid]["mtx"] == '1': # show matrix status
                print('\t pid:',pid,' -- ',did_file_problem[pid]+" (matrix)")
            else:
                print('\t pid:',pid,' -- ',did_file_problem[pid])
        print('\t PID List:',','.join([str(n) for n in did_file_problem.keys()]))

        if args.show_dids:
            for pid, dids in did_file_problem_by_pid.items():
                print('\t pid: %s, dids: %s' % (pid, ', '.join(dids)))
        
    print()
    print('\t6) OTHER (rare -- Possible DID mis-match or case difference for metadata -- re-build may or may not help):')
    if not len(other_problem):
        print('\t **Clean**')
    else:
        for pid in other_problem:
            if project_lookup[pid]["perm"] == '0':   # no temporary projects
                pass
            elif project_lookup[pid]["mtx"] == '1': # show matrix status
                print('\t pid:',pid,' -- ',other_problem[pid]+" (matrix)")
            else:
                print('\t pid:',pid,' -- ',other_problem[pid])
        print('\t PID List:',','.join([str(n) for n in other_problem.keys()]))
    
    print()
    if args.search_seqs:
        print('\t7) SEQUENCES (all-or-nothing: NO seqs found in `sequence_pdr_info` table):')
        if not len(missing_seqs):
            print('\t **Clean**')
        else:
            for pid in missing_seqs:
                if project_lookup[pid]["perm"] == '0':   # no temporary projects
                    pass
                elif project_lookup[pid]["mtx"] == '1': # show matrix status
                    print('\t pid:',pid,' -- ',missing_seqs[pid]+" (matrix)")
                else:
                    print('\t pid:',pid,' -- ',missing_seqs[pid])
                
            print('\t PID List:',','.join([str(n) for n in missing_seqs.keys()]))
        print()
    all_to_rebuild = list(other_problem.keys()) + list(mismatch_data.keys()) + list(no_file_found.keys()) + list(did_file_problem.keys()) 
     
    print("To rebuild: \ncd /groups/vampsweb/new_vamps_maintenance_scripts/; ./rebuild_vamps_files.py -host "+args.dbhost+" -pids %s; mail_done" % (",".join(list(set(all_to_rebuild)))))    
    print("Number of files that should be rebuilt:",len(other_problem)+len(mismatch_data)+len(no_file_found))
    print("No temporary projects shown")
    print('*'*60)

def read_original_metadata():
    file_path = os.path.join(args.json_file_path,NODE_DATABASE+'--metadata.json')
    try:
        with open(file_path) as data_file:
            data = json.load(data_file)
    except:
        print("could not read json file",file_path,'-Exiting')
        sys.exit(1)
    return data


def get_required_metadata_fields(args):
    q =  "SHOW fields from required_metadata_info"
    cur.execute(q)
    md_fields = []
    fields_not_wanted = ['required_metadata_id','dataset_id','created_at','updated_at']
    for row in cur.fetchall():
        if row[0] not in fields_not_wanted:
            md_fields.append(row[0])
    return md_fields

def get_project_lookup(args):
    q =  "SELECT dataset_id, dataset.project_id,  project, matrix, permanent from project"
    q += " JOIN dataset using(project_id) where project not like '%_Sgun' order by project"

    num = 0
    cur.execute(q)
   
    projects_by_did = {}
    project_id_lookup = {}
    project_lookup = {}

    for row in cur.fetchall():
        did = str(row[0])
        pid = str(row[1])
        pj  = row[2]
        mtx = str(row[3])
        perm = str(row[4])
        
        project_lookup[pid]  = {"p":pj, "mtx":mtx, "perm":perm}
        projects_by_did[did] = pj
        if pid in project_id_lookup:
            project_id_lookup[pid].append(str(did))
        else:
            project_id_lookup[pid] = [str(did)]

    return (projects_by_did, project_id_lookup, project_lookup)
#
#
#
if __name__ == '__main__':

    myusage = """
    
    metadata_files_synchrony.py
    
        -host/--host        vampsdb, vampsdev    dbhost:  [Default: localhost]
        
        -v/--verbose    lots of talk          
        
        -pid/--pid       will check a single pid for file consistancy
        
        -json_file_path/--json_file_path
        
        -s/--search_seqs  
    
    """

    parser.add_argument("-json_file_path", "--json_file_path",
                required=False,  action='store', dest = "json_file_path",  default='json',
                help="Not usually needed if -host is accurate")
    parser.add_argument("-host", "--host",
                required=False,  action='store', dest = "dbhost",  default='localhost',
                help="choices=['vampsdb','vampsdev','localhost']")
    parser.add_argument("-v", "--verbose",
                required=False,  action='store_true',  dest = "verbose",  default=False,
                help="")
    parser.add_argument("-pid", "--pid",
                required=False,  action='store',  dest = "single_pid",  default='',
                help="Will check a single pid for consistency")
    parser.add_argument("-dids", "--dids",
                required=False,  action='store_true',  dest = "show_dids",  default='',
                help="Show dids for 'Projects where the dataset file(s) are missing or corrupt'")
    parser.add_argument("-s", "--search_seqs",
                required=False,  action='store_true',  dest = "search_seqs",  default=False,
                help="search sequence_pdr_info for project sequence -- off by default'")
    if len(sys.argv[1:]) == 0:
        print(myusage)
        sys.exit()
    args = parser.parse_args()

    print(args)
    annas_local_hosts = ['Annas-MacBook.local', 'Annas-MacBook-new.local', 'micrcosmexp.mbl.edu']
    is_annas_localhost = socket.gethostname() in annas_local_hosts

    if args.dbhost == 'vamps' or args.dbhost == 'vampsdb' or args.dbhost == 'bpcweb8' :
        args.json_file_path = '/groups/vampsweb/vamps/nodejs/json'
        args.dbhost = 'vampsdb'
        args.NODE_DATABASE = 'vamps2'

    elif args.dbhost == 'vampsdev' or args.dbhost == 'bpcweb7':
        args.json_file_path = '/groups/vampsweb/vampsdev/nodejs/json'
        args.NODE_DATABASE = 'vamps2'
        args.dbhost = 'bpcweb7'
    elif args.dbhost == 'localhost' and is_annas_localhost:
        args.NODE_DATABASE = 'vamps2'
        args.dbhost = 'localhost'
    else:
        args.NODE_DATABASE = 'vamps_development'
        args.dbhost = 'localhost'
    # args.units = 'silva119'
#     args.units = 'rdp2.6'
#     args.units = 'generic'
#     if args.units == 'silva119':
#         args.files_prefix   = os.path.join(args.json_file_path, args.NODE_DATABASE+"--datasets_silva119")
#     elif args.units == 'rdp2.6':
#          args.files_prefix   = os.path.join(args.json_file_path, args.NODE_DATABASE+"--datasets_rdp2.6")
#     elif args.units == 'generic':
#          args.files_prefix   = os.path.join(args.json_file_path, args.NODE_DATABASE+"--datasets_generic")
#     else:
#         sys.exit('UNITS ERROR: '+args.units)

    if os.path.exists(args.json_file_path):
        print('Validated: json file path')
    else:
        print(myusage)
        print("Could not find json directory: '",args.json_file_path,"'-Exiting")
        sys.exit(-1)
    print("\nARGS: dbhost  =",args.dbhost)
    print("ARGS: json_dir=",args.json_file_path)

    db = mysql.connect(host=args.dbhost, # your host, usually localhost
                             read_default_file="~/.my.cnf_node"  )
    cur = db.cursor()
    if args.NODE_DATABASE:
        NODE_DATABASE = args.NODE_DATABASE
    else:
        cur.execute("SHOW databases like 'vamps%'")
        dbs = []
        print(myusage)
        db_str = ''
        for i, row in enumerate(cur.fetchall()):
            dbs.append(row[0])
            db_str += str(i)+'-'+row[0]+';  '
        print(db_str)
        db_no = input("\nchoose database number: ")
        if int(db_no) < len(dbs):
            NODE_DATABASE = dbs[db_no]
        else:
            sys.exit("unrecognized number -- Exiting")

    print()
    cur.execute("USE "+NODE_DATABASE)

    #out_file = "tax_counts--"+NODE_DATABASE+".json"
    #in_file  = "../json/tax_counts--"+NODE_DATABASE+".json"
    
    print('DATABASE:',NODE_DATABASE)

    go_list(args)
