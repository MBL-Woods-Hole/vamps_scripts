#!/usr/bin/env python

""" 
  create_counts_lookup.py


"""

import sys, os, io
import argparse

try:
    import pymysql as MySQLdb
except ImportError:
    import MySQLdb
except:
    raise

import json
import shutil
import datetime
import socket
from collections import defaultdict
import time

class MyConnection:
    """
    Takes parameters from ~/.my.cnf, default host = "vampsdev", db="test"
    if different use my_conn = MyConnection(host, db)
    """

    def __init__(self, host="bpcweb7", db="test", read_default_file=""):
        # , read_default_file=os.path.expanduser("~/.my.cnf"), port = 3306

        self.conn = None
        self.cursor = None
        self.cursorD = None
        self.rows = 0
        self.new_id = None
        self.lastrowid = None

        port_env = 3306
        try:
            print "host = " + str(host) + ", db = " + str(db)
            print "=" * 40
            read_default_file = os.path.expanduser("~/.my.cnf")

            if is_local():
                host = "127.0.0.1"
                read_default_file = "~/.my.cnf_local"
            self.conn = MySQLdb.connect(host=host, db=db, read_default_file=read_default_file, port=port_env)
            self.cursor = self.conn.cursor()
            self.cursorD = self.conn.cursor(MySQLdb.cursors.DictCursor)

        except (AttributeError, MySQLdb.OperationalError):
            self.conn = MySQLdb.connect(host=host, db=db, read_default_file=read_default_file, port=port_env)
            self.cursor = self.conn.cursor()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            raise
        except:  # catch everything
            print "Unexpected:"
            print sys.exc_info()[0]
            raise  # re-throw caught exception

    @staticmethod
    def connect(host, db, read_default_file, port_env):
        return MySQLdb.connect(host=host, db=db, read_default_file=read_default_file, port=port_env)

    def execute_fetch_select(self, sql):
        if self.cursor:
            try:
                self.cursor.execute(sql)
                return self.cursor.fetchall()
            except:
                print "ERROR: query = %s" % sql
                raise

    def execute_no_fetch(self, sql):
        if self.cursor:
            self.cursor.execute(sql)
            self.conn.commit()
            return self.cursor._info


    def execute_fetch_select_dict(self, sql):
        if self.cursorD:
            try:
                self.cursorD.execute(sql)
                return self.cursorD.fetchall()
            except:
                print "ERROR: query = %s" % sql
                raise

def is_local():
    print os.uname()[1]
    dev_comps = ['ashipunova.mbl.edu', "as-macbook.home", "as-macbook.local", "Ashipunova.local",
                 "Annas-MacBook-new.local", "Annas-MacBook.local"]
    if os.uname()[1] in dev_comps:
        return True
    else:
        return False


today = str(datetime.date.today())

"""
silva119 MISSING from taxcount(silva119 only) or json(silva119 or rdp2.6) files:
ID: 416 project: DCO_ORC_Av6

rdp
ID: 284 project: KCK_NADW_Bv6
ID: 185 project: LAZ_DET_Bv3v4
ID: 385 project: LAZ_PPP_Bv3v5
ID: 278 project: LAZ_SEA_Bv6v4
ID: 213 project: LTR_PAL_Av6

SELECT sum(seq_count), dataset_id, domain_id, domain
FROM sequence_pdr_info
JOIN sequence_uniq_info USING(sequence_id)
JOIN silva_taxonomy_info_per_seq USING(silva_taxonomy_info_per_seq_id)
JOIN silva_taxonomy USING(silva_taxonomy_id)
JOIN domain USING(domain_id)
JOIN phylum USING(phylum_id)
where dataset_id = '426'
GROUP BY dataset_id, domain_id

SELECT sum(seq_count), dataset_id, domain_id, domain, phylum_id, phylum
FROM sequence_pdr_info
JOIN sequence_uniq_info USING(sequence_id)
JOIN silva_taxonomy_info_per_seq USING(silva_taxonomy_info_per_seq_id)
JOIN silva_taxonomy USING(silva_taxonomy_id)
JOIN domain USING(domain_id)
JOIN phylum USING(phylum_id)
where dataset_id = '426'
GROUP BY dataset_id, domain_id, phylum_id
"""

query_coreA = " FROM sequence_pdr_info"
# query_coreA += " JOIN sequence_uniq_info USING(sequence_id)"

query_core_join_silva119 = " JOIN silva_taxonomy_info_per_seq USING(sequence_id)"
query_core_join_silva119 += " JOIN silva_taxonomy USING(silva_taxonomy_id)"

query_core_join_rdp = " JOIN rdp_taxonomy_info_per_seq USING(rdp_taxonomy_info_per_seq_id)"
query_core_join_rdp += " JOIN rdp_taxonomy USING(rdp_taxonomy_id)"

where_part = " WHERE dataset_id in (%s)"

domain_queryA = "SELECT sum(seq_count), dataset_id, domain_id"

domain_queryB = where_part
domain_queryB += " GROUP BY dataset_id, domain_id"

phylum_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id"
phylum_queryB = where_part
phylum_queryB += " GROUP BY dataset_id, domain_id, phylum_id"

class_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id"
class_queryB = where_part
class_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id"

order_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id"
order_queryB = where_part
order_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id"

family_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id"
family_queryB = where_part
family_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id"

genus_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id"
genus_queryB = where_part
genus_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id"

species_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id"
species_queryB = where_part
species_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id"

strain_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id"
strain_queryB = where_part
strain_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id"

cust_pquery = "SELECT project_id, field_name from custom_metadata_fields WHERE project_id = '%s'"

end_group_query = " ORDER BY NULL"

queries = [{"rank": "domain", "queryA": domain_queryA, "queryB": domain_queryB},
           {"rank": "phylum", "queryA": phylum_queryA, "queryB": phylum_queryB},
           {"rank": "klass", "queryA": class_queryA, "queryB": class_queryB},
           {"rank": "order", "queryA": order_queryA, "queryB": order_queryB},
           {"rank": "family", "queryA": family_queryA, "queryB": family_queryB},
           {"rank": "genus", "queryA": genus_queryA, "queryB": genus_queryB},
           {"rank": "species", "queryA": species_queryA, "queryB": species_queryB},
           {"rank": "strain", "queryA": strain_queryA, "queryB": strain_queryB}
           ]


def convert_keys_to_string(dictionary):
    """Recursively converts dictionary keys to strings."""
    if not isinstance(dictionary, dict):
        return dictionary
    return dict((str(k), convert_keys_to_string(v)) for k, v in dictionary.items())


def get_all_dids_per_pid_dict():
    query = "SELECT project_id, group_concat(dataset_id) AS dids FROM dataset GROUP BY project_id ORDER BY NULL"
    res = myconn.execute_fetch_select(query)
    return dict((str(x[0]), str(x[1])) for x in res)

def get_dco_pids():
    query = "select project_id from project where project like 'DCO%'"
    rows = myconn.execute_fetch_select(query)
    pid_list = [str(row[0]) for row in rows]
    return ', '.join(pid_list)

def make_list_from_c_str(comma_string):
    m_list = comma_string.split(',')
    m_list = map(str.strip, m_list) # trim spaces if any
    # Uniquing list here
    return list(set(m_list))

def make_prefix(args, node_database):
    prefix = ""
    if args.units == 'silva119':
        prefix = os.path.join(args.json_file_path, node_database + '--datasets_silva119')
    elif args.units == 'rdp2.6':
        prefix = os.path.join(args.json_file_path, node_database + '--datasets_rdp2.6')

    if not os.path.exists(prefix):
        os.makedirs(prefix)
    print prefix
    return prefix

def delete_old_did_files(dids, prefix):
    # delete old did files if any
    for did in dids:
        pth = os.path.join(prefix, did + '.json')
        try:
            os.remove(pth)
        except:
            pass

def go_add(node_database, pids_str):
    from random import randrange
    start1 = time.time()
    all_dids_per_pid_dict = get_all_dids_per_pid_dict()
    elapsed1 = (time.time() - start1)
    print "get_all_dids_per_pid_dict time: %s s" % elapsed1

    counts_lookup = defaultdict(dict)

    start2 = time.time()
    prefix = make_prefix(args, node_database)
    elapsed2 = (time.time() - start2)
    print "make_prefix time: %s s" % elapsed2
    
    all_dids = []
    metadata_lookup = {}

    start3 = time.time()
    pid_list = make_list_from_c_str(pids_str)
    elapsed3 = (time.time() - start3)
    print "3) make_list_from_c_str time: %s s" % elapsed3

    start4 = time.time()

    for k, pid in enumerate(pid_list):
        start5 = time.time()
        dids = make_list_from_c_str(all_dids_per_pid_dict[pid])
        elapsed5 = (time.time() - start5)
        print "5) make_list_from_c_str time: %s s" % elapsed5

        all_dids += dids

        start7 = time.time()
        delete_old_did_files(dids, prefix)
        elapsed7 = (time.time() - start7)
        print "7) delete_old_did_files time: %s s" % elapsed7

        did_sql = ', '.join(dids)
            #", ".join('%s' % w for w in set(dids) if w is not None)


        start6 = time.time()
        # print counts_lookup
        for q in queries:
            if args.units == 'rdp2.6':
                query = q["queryA"] + query_coreA + query_core_join_rdp + q["queryB"] % did_sql + end_group_query
            elif args.units == 'silva119':
                query = q["queryA"] + query_coreA + query_core_join_silva119 + q["queryB"] % did_sql + end_group_query
            print 'PID =', pid, '(' + str(k + 1), 'of', str(len(pid_list)) + ')'
            print query

            dirs = []

            rows = myconn.execute_fetch_select(query)

            for row in rows:

                count = int(row[0])
                did = str(row[1])
                tax_id_str = ''

                start8 = time.time()
                tax_id_str = '_' + "_".join([str(k) for k in row[2:]])
                elapsed8 = (time.time() - start8)
                print "8) tax_id_str time: %s s" % elapsed8

                if tax_id_str in counts_lookup[did]:
                    # unless pid was duplicated on CL
                    sys.exit('We should not be here - Exiting')
                else:
                    counts_lookup[did][tax_id_str] = count

        elapsed6 = (time.time() - start6)
        print "for q in queries (print counts_lookup) time: %s s" % elapsed6

        metadata_lookup = go_custom_metadata(dids, pid, metadata_lookup)
    elapsed4 = (time.time() - start4)
    print "for k, pid in enumerate(pid_list) time: %s s" % elapsed4

    print('all_dids', all_dids)
    all_did_sql = "', '".join(all_dids)
    metadata_lookup = go_required_metadata(all_did_sql, metadata_lookup)

    if args.metadata_warning_only:
        for did in dids:
            if did in metadata_lookup:
                print 'metadata found for did', did
            else:
                print 'WARNING -- no metadata for did:', did
    else:

        write_json_files(prefix, all_dids, metadata_lookup, counts_lookup)

        rando = randrange(10000, 99999)
        write_all_metadata_file(metadata_lookup, rando)

        # only write here for default taxonomy: silva119
        # discovered this file is not used
        # if args.units == 'silva119':
        #    write_all_taxcounts_file(counts_lookup, rando)


def write_all_metadata_file(metadata_lookup, rando):
    original_metadata_lookup = read_original_metadata()
    md_file = os.path.join(args.json_file_path, NODE_DATABASE + "--metadata.json")

    if not args.no_backup:
        bu_file = os.path.join(args.json_file_path, NODE_DATABASE + "--metadata_" + today + '_' + str(rando) + ".json")
        print 'Backing up metadata file to', bu_file
        shutil.copy(md_file, bu_file)
    # print md_file
    for did in metadata_lookup:
        original_metadata_lookup[did] = metadata_lookup[did]

    # print(metadata_lookup)
    # f = open(md_file, 'w')
    #     try:
    #         json_str = json.dumps(original_metadata_lookup, ensure_ascii=False)
    #     except:
    #         json_str = json.dumps(original_metadata_lookup)

    with io.open(md_file, 'w', encoding='utf-8') as f:
        try:
            f.write(json.dumps(original_metadata_lookup))
        except:
            f.write(json.dumps(original_metadata_lookup, ensure_ascii=False))
        finally:
            pass
    print 'writing new metadata file'
    # f.write(json_str.encode('utf-8').strip()+"\n")
    f.close()


def write_all_taxcounts_file(counts_lookup, rando):
    original_counts_lookup = read_original_taxcounts()

    tc_file = os.path.join(args.json_file_path, NODE_DATABASE + "--taxcounts_silva119.json")
    if not args.no_backup:
        bu_file = os.path.join(args.json_file_path,
                               NODE_DATABASE + "--taxcounts_silva119" + today + '_' + str(rando) + ".json")
        print 'Backing up taxcount file to', bu_file
        shutil.copy(tc_file, bu_file)
    for did in counts_lookup:
        original_counts_lookup[did] = counts_lookup[did]
    json_str = json.dumps(original_counts_lookup)
    # print(json_str)
    f = open(tc_file, 'w')  # this will delete taxcounts file!
    print 'writing new taxcount file'
    f.write(json_str + "\n")
    f.close()


def write_json_files(prefix, dids, metadata_lookup, counts_lookup):
    for did in dids:
        file_path = os.path.join(prefix, str(did) + '.json')
        print 'writing new file', file_path
        f = open(file_path, 'w')
        # print
        # print did, counts_lookup[did]
        if did in counts_lookup:
            my_counts_str = json.dumps(counts_lookup[did])
        else:
            my_counts_str = json.dumps({})
        if did in metadata_lookup:
            try:
                my_metadata_str = json.dumps(metadata_lookup[did])
            except:
                my_metadata_str = json.dumps(metadata_lookup[did], ensure_ascii=False)
        else:
            print 'WARNING -- no metadata for dataset:', did
            my_metadata_str = json.dumps({})
        # f.write('{"'+str(did)+'":'+mystr+"}\n")
        f.write('{"taxcounts":' + my_counts_str + ', "metadata":' + my_metadata_str + '}' + "\n")
        f.close()


def go_required_metadata(did_sql, metadata_lookup):
    """
        metadata_lookup_per_dsid[dsid][metadataName] = value            

    """

    required_metadata_fields = get_required_metadata_fields()
    req_query = "SELECT dataset_id, " + ', '.join(
        required_metadata_fields) + " from required_metadata_info WHERE dataset_id in ('%s')"
    query = req_query % did_sql
    print(query)
    rows = myconn.execute_fetch_select(query)
    for row in rows:
        # cur.execute(query)
        # for row in cur.fetchall():
        did = str(row[0])
        if did not in metadata_lookup:
            metadata_lookup[did] = {}
        for x, f in enumerate(required_metadata_fields):
            value = row[x + 1]
            metadata_lookup[did][f] = str(value)

    return metadata_lookup


def get_required_metadata_fields():
    query = "SHOW fields from required_metadata_info"
    rows = myconn.execute_fetch_select(query)
    # cur.execute(q)
    md_fields = []
    fields_not_wanted = ['required_metadata_id', 'dataset_id', 'created_at', 'updated_at']
    for row in rows:
        # for row in cur.fetchall():
        if row[0] not in fields_not_wanted:
            md_fields.append(row[0])
    return md_fields


def go_custom_metadata(did_list, pid, metadata_lookup):
    custom_table = 'custom_metadata_' + pid
    query = "show tables like '" + custom_table + "'"

    table_exists = myconn.execute_fetch_select(query)

    if not table_exists:
        return metadata_lookup

    field_collection = ['dataset_id']
    cust_metadata_lookup = {}
    query = cust_pquery % pid

    r_d = myconn.execute_fetch_select_dict(query)
    rows = myconn.execute_fetch_select(query)
    for row in rows:

        pid = str(row[0])
        field = row[1]
        if field != 'dataset_id':
            field_collection.append(field.strip())


    cust_dquery = "SELECT `" + '`, `'.join(field_collection) + "` from " + custom_table

    rows = myconn.execute_fetch_select(cust_dquery)
    for row in rows:
        did = str(row[0])
        if did in did_list:

            for y, f in enumerate(field_collection):

                if f != 'dataset_id':
                    value = str(row[y])

                    if did in metadata_lookup:
                        metadata_lookup[did][f] = value
                    else:
                        metadata_lookup[did] = {}
                        metadata_lookup[did][f] = value
    return metadata_lookup


def read_original_taxcounts():
    file_path1 = os.path.join(args.json_file_path, NODE_DATABASE + '--taxcounts_silva119.json')
    try:
        with open(file_path1) as data_file:
            data = json.load(data_file)
    except:

        file_path2 = os.path.join(args.json_file_path, NODE_DATABASE + '--taxcounts.json')
        print "could not read json file", file_path1, 'Now Trying', file_path2
        try:
            with open(file_path2) as data_file:
                data = json.load(data_file)
        except:
            print "could not read json file", file_path2, '--Exiting'
            sys.exit(1)
    return data


def read_original_metadata():
    file_path = os.path.join(args.json_file_path, NODE_DATABASE + '--metadata.json')
    try:
        with open(file_path) as data_file:
            data = json.load(data_file)
    except:
        print "could not read json file", file_path, '-Exiting'
        sys.exit(1)
    return data


def get_dataset_ids(pid):
    query = "SELECT dataset_id from dataset where project_id='" + str(pid) + "'"

    rows = myconn.execute_fetch_select(query)
    numrows = len(rows)
    if numrows == 0:
        sys.exit('No data found for pid ' + str(pid))
    dids = [str(row[0]) for row in rows]

    return dids

def ask_current_database(databases):
    print myusage

    dbs = [str(i) + '-' + str(row[0]) for i, row in enumerate(databases)]
    db_str = ';  '.join(dbs)

    print db_str
    db_no = input("\nchoose database number: ")
    if int(db_no) < len(dbs):
        return dbs[db_no]
    else:
        sys.exit("unrecognized number -- Exiting")


#
#
#
if __name__ == '__main__':
    start0 = time.time()

    myusage = """
        -pids/--pids  [list of comma separated pids]
                
        -json_file_path/--json_file_path   json files path [Default: ../json]
        -host/--host        vampsdb, vampsdev    dbhost:  [Default: localhost]
        -units/--tax-units  silva119, or rdp2.6   [Default:silva119]
        
    count_lookup_per_dsid[dsid][rank][taxid] = count

    This script will add a project to ../json/<NODE-DATABASE>/<DATASET-NAME>.json JSON object
    But ONLY if it is already in the MySQL database.
    
    To add a new project to the MySQL database:
    If already GASTed:
        use ./upload_project_to_database.py in this directory
    If not GASTed
         use py_mbl_sequencing_pipeline custom scripts

    """

    parser = argparse.ArgumentParser(description="", usage=myusage)

    parser.add_argument("-pids", "--pids",
                        required=True, action="store", dest="pids_str", default='',
                        help="""ProjectID (used with -add)""")

    parser.add_argument("-no_backup", "--no_backup",
                        required=False, action="store_true", dest="no_backup", default=False,
                        help="""no_backup of group files: taxcounts and metadata""")
    parser.add_argument("-metadata_warning_only", "--metadata_warning_only",
                        required=False, action="store_true", dest="metadata_warning_only", default=False,
                        help="""warns of datasets with no metadata""")
    parser.add_argument("-json_file_path", "--json_file_path",
                        required=False, action='store', dest="json_file_path", default='../../json',
                        help="Not usually needed if -host is accurate")
    # for vampsdev"  /groups/vampsweb/vampsdev_node_data/json
    parser.add_argument("-host", "--host",
                        required=False, action='store', dest="dbhost", default='localhost',
                        help="choices=['vampsdb', 'vampsdev', 'localhost']")
    parser.add_argument("-units", "--tax_units",
                        required=False, action='store', choices=['silva119', 'rdp2.6'], dest="units",
                        default='silva119',
                        help="Default: 'silva119'; only other choice available is 'rdp2.6'")
    parser.add_argument("-dco", "--dco",
                        required=False, action='store_true', dest="dco", default=False,
                        help="")
    # if len(sys.argv[1:]) == 0:
    #     print myusage
    #     sys.exit()
    args = parser.parse_args()

    if args.dbhost == 'vamps' or args.dbhost == 'vampsdb':
        args.json_file_path = '/groups/vampsweb/vamps_node_data/json'
        dbhost = 'vampsdb'
        args.NODE_DATABASE = 'vamps2'

    elif args.dbhost == 'vampsdev':
        args.json_file_path = '/groups/vampsweb/vampsdev_node_data/json'
        args.NODE_DATABASE = 'vamps2'
        dbhost = 'bpcweb7'
    elif args.dbhost == 'localhost' and (
            socket.gethostname() == 'Annas-MacBook.local' or socket.gethostname() == 'Annas-MacBook-new.local'):
        args.NODE_DATABASE = 'vamps2'
        dbhost = 'localhost'
    else:
        dbhost = 'localhost'
        args.NODE_DATABASE = 'vamps_development'
    if args.units == 'silva119':
        args.files_prefix = os.path.join(args.json_file_path, args.NODE_DATABASE + "--datasets_silva119")
    elif args.units == 'rdp2.6':
        args.files_prefix = os.path.join(args.json_file_path, args.NODE_DATABASE + "--datasets_rdp2.6")
    else:
        sys.exit('UNITS ERROR: ' + args.units)
    print "\nARGS: dbhost  =", dbhost
    print "\nARGS: NODE_DATABASE  =", args.NODE_DATABASE
    print "ARGS: json_file_path =", args.json_file_path
    if os.path.exists(args.json_file_path):
        print '** Validated json_file_path **'
    else:
        print myusage
        print "Could not find json directory: '", args.json_file_path, "'-Exiting"
        sys.exit(-1)
    print "ARGS: units =", args.units

    database = args.NODE_DATABASE
    myconn = MyConnection(dbhost, database, read_default_file="~/.my.cnf_node")

    if args.NODE_DATABASE:
        NODE_DATABASE = args.NODE_DATABASE
    else:
        databases = myconn.execute_fetch_select("SHOW databases like 'vamps%'")
        NODE_DATABASE = ask_current_database(databases)

    myconn.execute_no_fetch("USE " + NODE_DATABASE)

    # out_file = "tax_counts--"+NODE_DATABASE+".json"
    # in_file  = "../json/tax_counts--"+NODE_DATABASE+".json"

    print 'DATABASE:', NODE_DATABASE

    if args.dco:
        args.pids_str = get_dco_pids()



    go_add(NODE_DATABASE, args.pids_str)
    elapsed0 = (time.time() - start0)
    print "total time: %s s" % elapsed0