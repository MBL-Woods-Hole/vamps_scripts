#!/usr/bin/env python

""" 
  create_counts_lookup.py


"""

import sys,os,io
import argparse
import pymysql as MySQLdb
import json
import shutil
import datetime
import socket

today     = str(datetime.date.today())


"""

"""


parser = argparse.ArgumentParser(description="") 
query_coreA = " FROM sequence_pdr_info" 
query_coreA += " JOIN sequence_uniq_info USING(sequence_id)"

query_core_join_silva119 = " JOIN silva_taxonomy_info_per_seq USING(silva_taxonomy_info_per_seq_id)"
query_core_join_silva119 += " JOIN silva_taxonomy USING(silva_taxonomy_id)" 
query_core_join_rdp = " JOIN rdp_taxonomy_info_per_seq USING(rdp_taxonomy_info_per_seq_id)"
query_core_join_rdp += " JOIN rdp_taxonomy USING(rdp_taxonomy_id)" 

domain_queryA = "SELECT sum(seq_count), dataset_id, domain_id"
#domain_query += query_core
domain_queryB = " WHERE dataset_id in ('%s')"
domain_queryB += " GROUP BY dataset_id, domain_id"

phylum_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id" 
#phylum_query += query_core
phylum_queryB = " WHERE dataset_id in ('%s')"
phylum_queryB += " GROUP BY dataset_id, domain_id, phylum_id"

class_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id" 
#class_query += query_core
class_queryB = " WHERE dataset_id in ('%s')"
class_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id"

order_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id"
#order_query += query_core
order_queryB = " WHERE dataset_id in ('%s')"
order_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id"

family_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id"
#family_query += query_core
family_queryB = " WHERE dataset_id in ('%s')"
family_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id"

genus_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id" 
#genus_query += query_core
genus_queryB = " WHERE dataset_id in ('%s')"
genus_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id"

species_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id" 
#species_query += query_core
species_queryB = " WHERE dataset_id in ('%s')"
species_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id"

strain_queryA = "SELECT sum(seq_count), dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id" 
#strain_query += query_core
strain_queryB = " WHERE dataset_id in ('%s')"
strain_queryB += " GROUP BY dataset_id, domain_id, phylum_id, klass_id, order_id, family_id, genus_id, species_id, strain_id"


cust_pquery = "SELECT project_id,field_name from custom_metadata_fields WHERE project_id = '%s'"

queries = [{"rank":"domain",    "queryA":domain_queryA, "queryB":domain_queryB},
           {"rank":"phylum",    "queryA":phylum_queryA, "queryB":phylum_queryB},
           {"rank":"klass",     "queryA":class_queryA,  "queryB":class_queryB},
           {"rank":"order",     "queryA":order_queryA,  "queryB":order_queryB},
           {"rank":"family",    "queryA":family_queryA, "queryB":family_queryB},
           {"rank":"genus",     "queryA":genus_queryA,  "queryB":genus_queryB},
           {"rank":"species",   "queryA":species_queryA,"queryB":species_queryB},
           {"rank":"strain",    "queryA":strain_queryA, "queryB":strain_queryB}
           ]
           
def convert_keys_to_string(dictionary):
    """Recursively converts dictionary keys to strings."""
    if not isinstance(dictionary, dict):
        return dictionary
    return dict((str(k), convert_keys_to_string(v)) 
        for k, v in dictionary.items())
        
def go_list(args):
    
    #
    file_dids = []
    if args.units == 'silva119':
        #dids from big file
        counts_lookup = convert_keys_to_string(read_original_taxcounts())
        file_dids = counts_lookup.keys()
    elif args.units == 'rdp2.6':
        #dids from individual files
        files_prefix = os.path.join(args.json_file_path,NODE_DATABASE+'--datasets_rdp2.6')
        for file in os.listdir(files_prefix):
            if file.endswith(".json"):
                file_dids.append(os.path.splitext(file)[0])
    metadata_lookup = convert_keys_to_string(read_original_metadata())
    metadata_dids = metadata_lookup.keys()
    #
    #print file_dids
    #print len(file_dids)           
    q =  "SELECT dataset_id, dataset.project_id, project from project"
    q += " JOIN dataset using(project_id) order by project"
    
    num = 0
    cur.execute(q)
    #print 'List of projects in: '+in_file
    projects = {}
    missing_bulk_silva119 = {}
    missing_files = {}
    
    missing_metadata = {}
    for row in cur.fetchall():
        did = str(row[0])
        pid = row[1]
        project = row[2]
        projects[project] = pid
        if did not in metadata_dids:
            missing_metadata[project] = pid
        if did not in file_dids:
            missing_files[project] = pid
        if args.units == 'silva119':
            file_path = os.path.join(args.json_file_path,NODE_DATABASE+'--datasets_silva119',did+'.json')
            #file_path = os.path.join(args.json_file_path,NODE_DATABASE+'--datasets',did+'.json')
            if not os.path.isfile(file_path):
                missing_bulk_silva119[project] = pid
        #print 'project:',row[0],' --project_id:',row[1]
    sort_p = sorted(projects.keys())
    print 'UNITS:',args.units
    for project in sort_p:  
        if project not in missing_files and project not in missing_bulk_silva119:
            print 'ID:',projects[project],"-",project
        num += 1
    print
    print args.units,'MISSING from metadata bulk file:'
    sort_md = sorted(missing_metadata.keys())
    for project in sort_md:
        print 'ID:',missing_metadata[project],"project:",project
    print
    
    print args.units,'MISSING from taxcount(silva119 only) bulk file:'
    sort_m = sorted(missing_bulk_silva119.keys())
    for project in sort_m:
        print 'ID:',missing_bulk_silva119[project],"project:",project
    print
    print args.units,'MISSING '+args.units+' files:'
    sort_m = sorted(missing_files.keys())
    for project in sort_m:
        print 'ID:',missing_files[project],"project:",project
    print
    print 'Number of Projects:',num
    

def get_dco_pids(args):

    query = "select project_id from project where project like 'DCO%'"
    cur.execute(query)
    rows = cur.fetchall()
    pid_list = []
    for row in rows:
        pid_list.append(str(row[0])) 
    
    return ','.join(pid_list)
       
def go_add(NODE_DATABASE, pids_str):
    from random import randrange
    counts_lookup = {}
    if args.units == 'silva119':
        prefix = os.path.join(args.json_file_path,NODE_DATABASE+'--datasets_silva119')
    elif args.units == 'rdp2.6':
        prefix = os.path.join(args.json_file_path,NODE_DATABASE+'--datasets_rdp2.6')
    
    if not os.path.exists(prefix):
        os.makedirs(prefix)
    print prefix
    all_dids = []
    metadata_lookup = {}
    
    pid_list = pids_str.split(',')
    # Uniquing list here
    pid_set = set(pid_list)
    pid_list = list(pid_set)
    for i,pid in enumerate(pid_list):
        dids = get_dataset_ids(pid) 
        all_dids += dids
        # delete old did files if any
        for did in dids:        
            pth = os.path.join(prefix,did+'.json')
            try:            
                os.remove(pth)
            except:
                pass
        did_sql = "','".join(dids)
        #print counts_lookup
        for q in queries:
            if args.units == 'rdp2.6':
                query = q["queryA"] + query_coreA + query_core_join_rdp + q["queryB"] % (did_sql)
            elif args.units == 'silva119':
                query = q["queryA"] + query_coreA + query_core_join_silva119 + q["queryB"] % (did_sql)
            print 'PID =',pid, '('+str(i+1),'of',str(len(pid_list))+')'
            print query

            dirs = []
            cur.execute(query)
            for row in cur.fetchall():
                #print row
                count = int(row[0])
                did = str(row[1])
                # if args.separate_taxcounts_files:
               #      dir = prefix + str(ds_id)
               #
               #      if not os.path.isdir(dir):
               #          os.mkdir(dir)
                
                #tax_id = row[2]
                #rank = q["rank"]
                tax_id_str = ''
                for k in range(2,len(row)):
                    tax_id_str += '_' + str(row[k])
                #print 'tax_id_str',tax_id_str
                if did in counts_lookup:
                    #sys.exit('We should not be here - Exiting')
                    if tax_id_str in counts_lookup[did]:
                    	# unless pid was duplicated on CL
                        sys.exit('We should not be here - Exiting')
                    else:
                        counts_lookup[did][tax_id_str] = count
                    
                else:
                    counts_lookup[did] = {}
                    counts_lookup[did][tax_id_str] = count
    
        metadata_lookup = go_custom_metadata(dids, pid, metadata_lookup)
    
    print('all_dids',all_dids)
    all_did_sql = "','".join(all_dids)
    metadata_lookup = go_required_metadata(all_did_sql,metadata_lookup)
    
    if args.metadata_warning_only:
        for did in dids:            
             if did in metadata_lookup:
                 print 'metadata found for did',did
             else:
                 print 'WARNING -- no metadata for did:',did
    else:
        
        
        write_json_files(prefix, all_dids, metadata_lookup, counts_lookup)
    
        rando = randrange(10000,99999)
        write_all_metadata_file(metadata_lookup, rando)
    
        # only write here for default taxonomy: silva119
        # discovered this file is not used
        #if args.units == 'silva119':
        #    write_all_taxcounts_file(counts_lookup, rando)
    

def write_all_metadata_file(metadata_lookup,rando):
    original_metadata_lookup = read_original_metadata()
    md_file = os.path.join(args.json_file_path,NODE_DATABASE+"--metadata.json")
    
    if not args.no_backup:
        bu_file = os.path.join(args.json_file_path,NODE_DATABASE+"--metadata_"+today+'_'+str(rando)+".json")
        print 'Backing up metadata file to',bu_file
        shutil.copy(md_file, bu_file)
    #print md_file
    for did in metadata_lookup:
        original_metadata_lookup[did] = metadata_lookup[did]
        
    #print(metadata_lookup)
    # f = open(md_file,'w')
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
    #f.write(json_str.encode('utf-8').strip()+"\n")
    f.close() 
    
def write_all_taxcounts_file(counts_lookup,rando):
    original_counts_lookup = read_original_taxcounts()
    
    tc_file = os.path.join(args.json_file_path,NODE_DATABASE+"--taxcounts_silva119.json")
    if not args.no_backup:
        bu_file = os.path.join(args.json_file_path,NODE_DATABASE+"--taxcounts_silva119"+today+'_'+str(rando)+".json")
        print 'Backing up taxcount file to',bu_file
        shutil.copy(tc_file, bu_file)
    for did in counts_lookup:
        original_counts_lookup[did] = counts_lookup[did]
    json_str = json.dumps(original_counts_lookup)       
    #print(json_str)
    f = open(tc_file,'w')  # this will delete taxcounts file!
    print 'writing new taxcount file'
    f.write(json_str+"\n")
    f.close()
      
def write_json_files(prefix, dids, metadata_lookup, counts_lookup):
    #json_str = json.dumps(counts_lookup)    
    # print('Re-Writing JSON file (REMEMBER to move new file to ../json/)')
    # f = open(outfile,'w')
    # f.write(json_str+"\n")
    # f.close()
    # for did in counts_lookup:
#         file_path = os.path.join(prefix,str(did)+'.json')
#         f = open(file_path,'w')
#         mystr = json.dumps(counts_lookup[did])
#         print mystr
#         f.write('{"'+str(did)+'":'+mystr+"}\n")
#         f.close()
     for did in dids:
         file_path = os.path.join(prefix,str(did)+'.json')
         print 'writing new file',file_path
         f = open(file_path,'w') 
         #print
         #print did, counts_lookup[did]
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
             print 'WARNING -- no metadata for dataset:',did
             my_metadata_str = json.dumps({})
         #f.write('{"'+str(did)+'":'+mystr+"}\n") 
         f.write('{"taxcounts":'+my_counts_str+',"metadata":'+my_metadata_str+'}'+"\n")
         f.close()   
def go_required_metadata(did_sql, metadata_lookup):
    """
        metadata_lookup_per_dsid[dsid][metadataName] = value            

    """
    
    required_metadata_fields = get_required_metadata_fields(args)
    req_query = "SELECT dataset_id, "+','.join(required_metadata_fields)+" from required_metadata_info WHERE dataset_id in ('%s')"
    query = req_query % (did_sql)
    print(query)
    cur.execute(query)
    for row in cur.fetchall():
        did = str(row[0])
        if did not in metadata_lookup:              
            metadata_lookup[did] = {}
        #metadata_lookup[did]['primer_id'] = []
        for i,f in enumerate(required_metadata_fields):
            #print i,did,name,row[i+1]
            value = row[i+1]
# DO not put primers or primer_ids into files
#             if f == 'primer_suite_id':
#                 primer_query = "SELECT primer_id from primer"
#                 primer_query += " join ref_primer_suite_primer using(primer_id)"
#                 primer_query += " WHERE primer_suite_id='%s'"
#                 pquery = primer_query % (value)
#                 #print(pquery)
#                 cur.execute(pquery)
#                 metadata_lookup[did]['primer_ids'] = []
#                 for primer_row in cur.fetchall():
#                     metadata_lookup[did]['primer_ids'].append(str(primer_row[0]))
                     
            metadata_lookup[did][f] = str(value)
                
    
    return metadata_lookup

    
def get_required_metadata_fields(args):
    q =  "SHOW fields from required_metadata_info"   
    cur.execute(q)
    md_fields = []
    fields_not_wanted = ['required_metadata_id','dataset_id','created_at','updated_at']    
    for row in cur.fetchall():
        if row[0] not in fields_not_wanted:
            md_fields.append(row[0])
    return md_fields
        
def go_custom_metadata(did_list, pid, metadata_lookup):
    
    custom_table = 'custom_metadata_'+ pid
    q = "show tables like '"+custom_table+"'"
    cur.execute(q)
    table_exists = cur.fetchall()
    if not table_exists:
        return metadata_lookup
    
    field_collection = ['dataset_id']
    cust_metadata_lookup = {}
    query = cust_pquery % (pid)
    cur.execute(query)
    for row in cur.fetchall():
        pid = str(row[0])
        field = row[1]
        if field != 'dataset_id':
            field_collection.append(field.strip())
        
    
    #print 'did_list',did_list
    #print 'field_collection',field_collection

    cust_dquery = "SELECT `" + '`,`'.join(field_collection) + "` from " + custom_table
    #print cust_dquery
    #try:
    cur.execute(cust_dquery)

    #print 'metadata_lookup1',metadata_lookup
    for row in cur.fetchall():
        #print row
        did = str(row[0])
        if did in did_list:
            
            
            for i,f in enumerate(field_collection):
                #cnt = i
                
                if f != 'dataset_id':
                    #if row[i]:
                    value = str(row[i])
                    #else:
                    #    value = None
                    #print 'XXX',did,i,f,value

                    if did in metadata_lookup:              
                        metadata_lookup[did][f] = value
                    else:
                        metadata_lookup[did] = {}
                        metadata_lookup[did][f] = value
                
        #except:
        #    print 'could not find or read',table,'Skipping'
    print
    #print 'metadata_lookup2',metadata_lookup
    #sys.exit()
    return metadata_lookup
    
def read_original_taxcounts():
    file_path1 = os.path.join(args.json_file_path,NODE_DATABASE+'--taxcounts_silva119.json')
    try:
        with open(file_path1) as data_file:    
            data = json.load(data_file) 
    except:
        
        file_path2 = os.path.join(args.json_file_path,NODE_DATABASE+'--taxcounts.json')
        print "could not read json file",file_path1,'Now Trying',file_path2
        try:    
            with open(file_path2) as data_file:    
                data = json.load(data_file) 
        except:
            print "could not read json file",file_path2,'--Exiting'
            sys.exit(1)
    return data 
     
def read_original_metadata():    
    file_path = os.path.join(args.json_file_path,NODE_DATABASE+'--metadata.json')
    try:
        with open(file_path) as data_file:    
            data = json.load(data_file)
    except:
        print "could not read json file",file_path,'-Exiting'
        sys.exit(1)
    return data 

def go(args):
    
    #file_type = 'dataset' # dataset   sequence
    # A 921
    # B 845
    if args.file_type == 'dataset':
        out_file = 'dataset_'+args.project+'.csv'
        dataset_index=0 # for dataset file
    else:   # seqs
        out_file = 'sequences_'+args.project+'.csv'
        dataset_index = 3
    f_out = open(out_file,'w')
    
    n=0
    ds_collector = []
    
    with open(args.infile) as in_file:
        for line in in_file:
            n += 1
            if n == 1:
                f_out.write(line)
            else:
                line = line.strip()
                line_items = [x.strip('"') for x in line.split(',')]
                #print line_items
                
                if line_items[dataset_index] in args.dataset_list:
                    ds_collector.append(line_items[dataset_index])
                    f_out.write(line.replace('JCM_COBR_Bv4v5',args.project)+'\n')
    for ds in args.dataset_list:
        if ds not in ds_collector:
            print "Missing",ds
            
    f_out.close()  
            
                  
def get_datasets(args):
    datasets = []
    n=0
    with open(args.dataset_file) as data_file: 
        for f in data_file:
            datasets = f.strip().split(',')
    print 'DS Count',len(datasets)    
    return datasets
    

#
#
#
if __name__ == '__main__':

    myusage = """
       ./split_export_file.py -p JCM_NHBCS_Bv4v5_20130619_A -d datasets.txt -i ../sequences_JCM_COBR_Bv4v5.csv -type sequences
       
       -type/--type  [dataset, sequences]

    """
    parser.add_argument("-p","--project",                   
                required=False,  action="store",   dest = "project", default='',
                help="""ProjectID (used with -add) no response if -list also included""") 
        
    
    parser.add_argument("-i", "--infile",    
                required=False,  action='store', dest = "infile",  default='',
                help="") 
                        
    parser.add_argument("-d", "--dataset_file",    
                required=False,  action='store',  dest = "dataset_file",  default=False,
                help="")   
    parser.add_argument("-type", "--type",    
                required=True,  action='store',  dest = "file_type",  
                help="")
    if len(sys.argv[1:]) == 0:
        print myusage
        sys.exit() 
    args = parser.parse_args()
    
    
    args.dataset_list = get_datasets(args)
    #print args.dataset_list
    go(args)
    
        

