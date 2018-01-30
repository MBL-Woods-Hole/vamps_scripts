#!/usr/bin/env python

""" 
  create_counts_lookup.py


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
import logging


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
            read_default_file = os.path.expanduser("~/.my.cnf")

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

cust_pquery = "SELECT field_name from custom_metadata_fields WHERE project_id = '%s'"

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


def get_all_dids_per_pid_dict():
    query = "SELECT project_id, group_concat(dataset_id) AS dids FROM dataset GROUP BY project_id ORDER BY NULL"
    res = myconn.execute_fetch_select(query)
    return dict((str(x[0]), make_list_from_c_str(x[1])) for x in res)


def get_dco_pids():
    query = "select project_id from project where project like 'DCO%'"
    rows = myconn.execute_fetch_select(query)
    pid_list = [str(row[0]) for row in rows]
    return ', '.join(pid_list)


def make_list_from_c_str(comma_string):
    m_list = comma_string.split(',')
    m_list = map(str.strip, m_list)  # trim spaces if any
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
    print(prefix)
    return prefix


def delete_old_did_files(dids, prefix):
    # delete old did files if any
    for did in dids:
        pth = os.path.join(prefix, did + '.json')
        try:
            os.remove(pth)
        except:
            pass


def make_list_chunks(my_list, chunk_size):
    chunk_size = int(chunk_size)
    return [my_list[x:x + chunk_size] for x in range(0, len(my_list), chunk_size)]


def get_counts_per_tax(did_sql, units):
    counts_per_tax_dict = {}
    for q in queries:
        if units == 'silva119':
            query = q["queryA"] + query_coreA + query_core_join_silva119 + q["queryB"] % did_sql + end_group_query
        elif units == 'rdp2.6':
            query = q["queryA"] + query_coreA + query_core_join_rdp + q["queryB"] % did_sql + end_group_query
        try:
            logging.debug("running mysql query for: "+q['rank'])

            rows = myconn.execute_fetch_select(query)
            rank = q['rank']
            counts_per_tax_dict[rank] = rows
        except:
            logging.debug("Failing to query with: "+query)
            sys.exit("This Database Doesn't Look Right -- Exiting")
    return counts_per_tax_dict


def make_counts_lookup_dict(counts_per_tax_dict, counts_lookup):

    for rank, res in counts_per_tax_dict.items():
        for row in res:
            count = int(row[0])
            ds_id = row[1]
            tax_id_str = '_' + "_".join([str(k) for k in row[2:]])

            if tax_id_str in counts_lookup[ds_id]:  #? Andy
                    sys.exit('We should not be here - Exiting')
            counts_lookup[ds_id][tax_id_str] = count
    return counts_lookup


def make_counts_lookup_by_did(did_list_group, units):
    counts_lookup = defaultdict(dict)

    for short_list in did_list_group:
        did_sql = ', '.join(short_list)
        counts_per_tax_dict = get_counts_per_tax(did_sql, units)
        counts_lookup = make_counts_lookup_dict(counts_per_tax_dict, counts_lookup)

    return counts_lookup


def make_metadata_by_pid(pid_list_group, all_dids_per_pid_dict):
    metadata_lookup = defaultdict(dict)

    for short_list in pid_list_group:
        for pid in short_list:
            dids = all_dids_per_pid_dict[pid]
            metadata_lookup = go_custom_metadata(dids, pid, metadata_lookup)

    return metadata_lookup


def go_add(node_database, pids_str, all_pids):
    all_dids_per_pid_dict = get_all_dids_per_pid_dict()

    pid_list = make_list_from_c_str(pids_str)
    if all_pids:
        pid_list = list(all_dids_per_pid_dict.keys())
    group_size = 1000

    pid_list_group = make_list_chunks(pid_list, group_size)
    all_used_dids = get_all_used_dicts(all_dids_per_pid_dict, pid_list)
    did_list_group = make_list_chunks(all_used_dids, group_size)

    start1 = time.time()
    counts_lookup = make_counts_lookup_by_did(did_list_group, args.units)
    elapsed1 = (time.time() - start1)
    print("1) make_counts_lookup_by_did time: %s s" % elapsed1)

    start2 = time.time()
    metadata_lookup = make_metadata_by_pid(pid_list_group, all_dids_per_pid_dict)
    elapsed2 = (time.time() - start2)
    print("2) make_metadata_by_pid time: %s s" % elapsed2)

    print('all_used_dids', all_used_dids)
    all_did_sql = "', '".join(all_used_dids)
    metadata_lookup = go_required_metadata(all_did_sql, metadata_lookup)

    start3 = time.time()
    show_result(node_database, metadata_lookup, counts_lookup, all_used_dids)
    elapsed3 = (time.time() - start3)
    print("3) show_result time: %s s" % elapsed3)


def get_all_used_dicts(all_dids_per_pid_dict, pid_list):
    all_used_dids = []
    for pid in pid_list:
        try:
            all_used_dids.extend(all_dids_per_pid_dict[pid])
        except KeyError:
            print("WARNING: There is no project with id = %s" % pid)
            continue
        except:
            raise
    return all_used_dids


def show_result(node_database, metadata_lookup, counts_lookup, all_used_dids):
    from random import randrange

    if args.metadata_warning_only:
        for did in all_used_dids:
            if did in metadata_lookup:
                print('metadata found for did', did)
            else:
                print('WARNING -- no metadata for did:', did)
    else:
        prefix = make_prefix(args, node_database)

        delete_old_did_files(all_used_dids, prefix)

        write_json_files(prefix, all_used_dids, metadata_lookup, counts_lookup)

        rando = randrange(10000, 99999)
        write_all_metadata_file(metadata_lookup, rando)


def write_all_metadata_file(metadata_lookup, rando):
    original_metadata_lookup = read_original_metadata()
    md_file = os.path.join(args.json_file_path, NODE_DATABASE + "--metadata.json")

    if not args.no_backup:
        bu_file = os.path.join(args.json_file_path, NODE_DATABASE + "--metadata_" + today + '_' + str(rando) + ".json")
        print('Backing up metadata file to', bu_file)
        shutil.copy(md_file, bu_file)
    # print(md_file)
    for did in metadata_lookup:
        original_metadata_lookup[did] = metadata_lookup[did]

    with io.open(md_file, 'w', encoding = 'utf-8') as f:
        try:
            f.write(json.dumps(original_metadata_lookup))
        except:
            f.write(json.dumps(original_metadata_lookup, ensure_ascii = False))
        finally:
            pass
    print('writing new metadata file')
    # f.write(json_str.encode('utf-8').strip()+"\n")
    f.close()


def write_json_files(prefix, dids, metadata_lookup, counts_lookup):
    for did in dids:
        file_path = os.path.join(prefix, str(did) + '.json')
        print('writing new file', file_path)
        f = open(file_path, 'w')
        # print
        # print(did, counts_lookup[did])
        if did in counts_lookup:
            my_counts_str = json.dumps(counts_lookup[did])
        else:
            my_counts_str = json.dumps({})
        if did in metadata_lookup:
            try:
                my_metadata_str = json.dumps(metadata_lookup[did])
            except:
                my_metadata_str = json.dumps(metadata_lookup[did], ensure_ascii = False)
        else:
            print('WARNING -- no metadata for dataset:', did)
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


def make_field_names_by_pid_list(pid):
    query = cust_pquery % pid

    rows = myconn.execute_fetch_select(query)
    return set([x[0] for x in rows] + ['dataset_id'])


def go_custom_metadata(did_list, pid, metadata_lookup):
    custom_table = 'custom_metadata_' + pid
    query = "show tables like '" + custom_table + "'"

    table_exists = myconn.execute_fetch_select(query)

    if not table_exists:
        return metadata_lookup

    field_collection = make_field_names_by_pid_list(pid)

    cust_dquery = "SELECT `" + '`, `'.join(field_collection) + "` from " + custom_table

    rows_dict = myconn.execute_fetch_select_dict(cust_dquery)

    for row in rows_dict:
        did = str(row['dataset_id'])
        if did in did_list:
            for field, val in row.items():
                metadata_lookup[did][field] = val

    return metadata_lookup


def read_original_taxcounts():
    file_path1 = os.path.join(args.json_file_path, NODE_DATABASE + '--taxcounts_silva119.json')
    try:
        with open(file_path1) as data_file:
            data = json.load(data_file)
    except:

        file_path2 = os.path.join(args.json_file_path, NODE_DATABASE + '--taxcounts.json')
        print("could not read json file", file_path1, 'Now Trying', file_path2)
        try:
            with open(file_path2) as data_file:
                data = json.load(data_file)
        except:
            print("could not read json file", file_path2, '--Exiting')
            sys.exit(1)
    return data


def read_original_metadata():
    file_path = os.path.join(args.json_file_path, NODE_DATABASE + '--metadata.json')
    try:
        with open(file_path) as data_file:
            data = json.load(data_file)
    except:
        print("could not read json file", file_path, '-Exiting')
        sys.exit(1)
    return data


def ask_current_database(databases):
    print(myusage)

    dbs = [str(i) + '-' + str(row[0]) for i, row in enumerate(databases)]
    db_str = ';  '.join(dbs)

    print(db_str)
    db_no = input("\nchoose database number: ")
    if int(db_no) < len(dbs):
        return dbs[db_no]
    else:
        sys.exit("unrecognized number -- Exiting")


if __name__ == '__main__':
    start0 = time.time()

    myusage = """
        Required:
        -pids  [list of comma separated pids]
        OR
        -a (All project and dataset files will be updated)
        
        Optional:
        -json_file_path  json files path [Default: ../json]
        -host            vampsdb, vampsdev    dbhost:  [Default: localhost]
        -units           silva119, or rdp2.6   [Default:silva119]

    count_lookup_per_did[did][rank][taxid] = count

    This script will add a project to ../json/<NODE-DATABASE>/<DATASET-NAME>.json JSON object
    But ONLY if it is already in the MySQL database.

    To add a new project to the MySQL database:
    If already GASTed:
        use ./upload_project_to_database.py in this directory
    If not GASTed
         use py_mbl_sequencing_pipeline custom scripts

    """

    parser = argparse.ArgumentParser(description = "", usage = myusage)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-pids", "--pids",
                        action = "store", dest = "pids_str", default = '',
                        help = """ProjectID (used with -add)""")
    group.add_argument("-a", "--all",
                        action = 'store_true', dest = "all_pids", default = False,
                        help = "All project and dataset files will be updated")

    parser.add_argument("-no_backup", "--no_backup",
                        required = False, action = "store_true", dest = "no_backup", default = False,
                        help = """no_backup of group files: taxcounts and metadata""")
    parser.add_argument("-metadata_warning_only", "--metadata_warning_only",
                        required = False, action = "store_true", dest = "metadata_warning_only", default = False,
                        help = """warns of datasets with no metadata""")
    parser.add_argument("-json_file_path", "--json_file_path",
                        required = False, action = 'store', dest = "json_file_path", default = '../../json',
                        help = "Not usually needed if -host is accurate")
    # for vampsdev"  /groups/vampsweb/vampsdev_node_data/json
    parser.add_argument("-host", "--host",
                        required = False, action = 'store', dest = "dbhost", default = 'localhost',
                        help = "choices=['vampsdb', 'vampsdev', 'localhost']")
    parser.add_argument("-units", "--tax_units",
                        required = False, action = 'store', choices = ['silva119', 'rdp2.6'], dest = "units",
                        default = 'silva119',
                        help = "Default: 'silva119'; only other choice available is 'rdp2.6'")
    parser.add_argument("-dco", "--dco",
                        required = False, action = 'store_true', dest = "dco", default = False,
                        help = "")

    # if len(sys.argv[1:]) == 0:
    #     print(myusage)
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
    print("\nARGS: dbhost  =", dbhost)
    print("\nARGS: NODE_DATABASE  =", args.NODE_DATABASE)
    print("ARGS: json_file_path =", args.json_file_path)
    if os.path.exists(args.json_file_path):
        print('** Validated json_file_path **')
    else:
        print(myusage)
        print("Could not find json directory: '", args.json_file_path, "'-Exiting")
        sys.exit(-1)
    print("ARGS: units = ", args.units)

    database = args.NODE_DATABASE
    myconn = MyConnection(dbhost, database, read_default_file = "~/.my.cnf_node")

    if args.NODE_DATABASE:
        NODE_DATABASE = args.NODE_DATABASE
    else:
        databases = myconn.execute_fetch_select("SHOW databases like 'vamps%'")
        NODE_DATABASE = ask_current_database(databases)

    # out_file = "tax_counts--"+NODE_DATABASE+".json"
    # in_file  = "../json/tax_counts--"+NODE_DATABASE+".json"

    print('DATABASE:', NODE_DATABASE)

    if args.dco:
        args.pids_str = get_dco_pids()

    go_add(NODE_DATABASE, args.pids_str,  args.all_pids)
    elapsed0 = (time.time() - start0)
    print("total time: %s s (~ %s m)" % (elapsed0, float(elapsed0)/60))
