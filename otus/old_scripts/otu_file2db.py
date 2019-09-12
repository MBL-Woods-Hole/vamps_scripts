#!/usr/bin/env python


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

import csv
import json
from time import sleep
import pymysql as MySQLdb
import ConMySQL as db
import datetime

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
def run_biom(myobject):

    
    biom_file = data_object['biomfile']
    filepath = data_object['filepath']
    user = myobject['user']
   
    cursor = myobject['cursor']
   
    run = myobject['run']
    
    
    
    otu_prefix = user+'_'+run
    project = otu_prefix
   
    project_info_table = "otu_project_info"
    pd_table = "otu_projects_datasets"
    #seqs_table = "otu_sequences"
    tax_table = "otu_taxonomy"
    otu_table = 'otu_user_clusters'
    
    
    datetime = data_object['datetime']
    from_vamps = data_object['from_vamps']
    
    tax_name={}
    seq_name={}
    rank={}
    cluster_size={}
    otu=[]
    otu_taxonomy={}
    dataset=[]
    ds_count={}
    if os.path.exists(biom_file):
        with open(biom_file, 'r') as json_data:
            biomdata = json.load(json_data)
        
        row_size = biomdata['shape'][0]
        col_size = biomdata['shape'][1]
        info_file = os.path.join(filepath,'info.txt')
        fh = open(info_file,'w')
        fh.write("project = %s\n" % (project))
        fh.write("create_date = %s\n" % (today))
        fh.write("clustering_method = unknown\n")
        fh.write("data_source = BiomFile\n")
        fh.write("otu_size = \n")
        fh.write("sequence_count = \n")
        fh.write("otu_count = %s\n" % (str(row_size)))
        # create empty matrix with zeros:
        data_matrix=  [[0 for x in xrange(col_size)] for x in xrange(row_size)] 
        
        
        
        for i,row_arr in enumerate(biomdata['rows']):
            #print i, row_arr
            otu_id = row_arr['id']
            new_otu_id = otu_prefix+'_'+otu_id
            otu.append(new_otu_id)
            tax='unknown'
            if row_arr['metadata'] and 'taxonomy' in row_arr['metadata']:
                tax_available=True
                if type(row_arr['metadata']['taxonomy']) is list:
                    split_tax = row_arr['metadata']['taxonomy']
                elif type(row_arr['metadata']['taxonomy']) is str:
                    split_tax = row_arr['metadata']['taxonomy'].split(';')
                else:
                    print 'Error Empty Tax'
                    split_tax = []
                #print split_tax[0]
                if split_tax[0].find('__',1) == 1:
                    #print 'found __'
                    # will be -1 if not found
                    # some qiime taxonomy islike this 
                    # 	k__Bacteria	 p__Proteobacteria	 c__Gammaproteobacteria	 o__Legionellales	 f__Legionellaceae	 g__Legionella	 s__
                    taxlist=[]
                    
                    for t in split_tax:
                        #print t.strip().split('__')[1]
                        if t.strip().split('__')[1]:
                            taxlist.append(t.strip().split('__')[1])
                    tax = ';'.join(taxlist)
                else:            
                    tax = ';'.join(row_arr['metadata']['taxonomy'])
            else:
                tax_available=False
            
            otu_taxonomy[new_otu_id] = tax
            
            #otu_taxonomy.append(tax)
        if tax_available:
            fh.write("taxonomy_available = yes\n")
        else:
            fh.write("taxonomy_available = no\n")
            
        for i,col_arr in enumerate(biomdata['columns']):
            #print i, col_arr
            dataset.append(col_arr['id'])
            fh.write("dataset[] = %s\n" % (col_arr['id']))
            #print dataset[i]    
        for i,val_arr in enumerate(biomdata['data']):
            # [0, 0, 1.0] = [row_index(otu), col_index(dataset), value]
                        
            data_matrix[val_arr[0]][val_arr[1]]=val_arr[2]
            ds_name = dataset[val_arr[1]]
            
            # sum across the datasets
            if ds_name in ds_count:
                ds_count[ds_name] += val_arr[2]
            else:
                ds_count[ds_name] = val_arr[2]
        fh.close()
        
    print "\n\nFinished Gathering Data for "+project   
    load_db(cursor, dataset, otu, data_matrix, ds_count, otu_taxonomy, project, user, run, row_size,'biomfile')
#import pipeline.constants as C

def run_matrix(myobject):
    mtx_file = data_object['mtxfile']
    filepath = data_object['filepath']
    user = myobject['user']
   
    cursor = myobject['cursor']
   
    run = myobject['run']
    
    otu_prefix = user+'_'+run
    project = otu_prefix
   
    project_info_table = "otu_project_info"
    pd_table = "otu_projects_datasets"
    #seqs_table = "otu_sequences"
    tax_table = "otu_taxonomy"
    otu_table = 'otu_user_clusters'
    
    
    datetime = data_object['datetime']
    from_vamps = data_object['from_vamps']
    
    tax_name={}
    seq_name={}
    rank={}
    cluster_size={}
    otu_count=0
    otu=[]
    otu_taxonomy={}
    ds_counts={}
    dataset=[]
    otu_counts_list = {}
    got_datasets=False
    
    data_matrix=[]
    data_matrix=  [[0 for x in xrange(dataset_count)] for x in xrange(otu_count)] 
    # [0, 0, 1.0] = [row_index(otu), col_index(dataset), value]
    #data_matrix[val_arr[0]][val_arr[1]]=val_arr[2]
        
        
    if os.path.exists(mtx_file):
        # do we have taxonomy??
        f = open(mtx_file)
        for line in f:
            if line.startswith('#') or not line:
                continue
            line_items=line.strip().split("\t")
            if  line_items[0].strip().strip("'").strip('"') == "Cluster ID" or \
                line_items[0].strip().strip("'").strip('"') == "Cluster_ID" or \
                line_items[0].strip().strip("'").strip('"') == "ClusterID" or \
                line_items[0].strip().strip("'").strip('"') == "OTUID" or \
                line_items[0].strip().strip("'").strip('"') == "OTU_ID" or \
                line_items[0].strip().strip("'").strip('"') == "OTU ID":
                
                # got required header line with dataset names and potentially taxonomy
                dataset_count=len(line_items)-1
                if line_items[len(line_items)-1].strip().strip("'").strip('"')=="Taxonomy":
                    tax_available=True
                    dataset_count=len(line_items)-2
                else:
                    tax_available=False
                for n in range(1,dataset_count+1):
                    dataset.append(line_items[n].strip().strip("'").strip('"'))
                got_datasets=True
            else:
                if not got_datasets:
                    sys.exit("No dataset names found exiting")
                #print otu_count
                data_matrix.append(otu_count)
                data_matrix[otu_count]=[]
                otu_name = otu_prefix+'_'+line_items[0].strip().strip("'").strip('"') 
                otu.append(otu_name)
                otu_counts_list[otu_name]   = line_items[1:dataset_count+1] # list of dataset counts
                ds_counter=1
                for i,ds in enumerate(dataset):
                    if ds in ds_counts:
                        ds_counts[ds] += int(line_items[i+1])
                    else:
                        ds_counts[ds] = int(line_items[i+1])
                        
                    #print 'row ',otu_count,'col ',i,'val ',line_items[i+1]
                    data_matrix[otu_count].append(i)
                    data_matrix[otu_count][i]=line_items[i+1]
                    
                    
                if tax_available:
                    otu_taxonomy[otu_name]= line_items[len(line_items)-1].strip().strip("'").strip('"') 
                else:
                    otu_taxonomy[otu_name]= 'unknown' 
                otu_count+=1
                    
            
        f.close()
        
        info_file = os.path.join(filepath,'info.txt')
        fh = open(info_file,'w')
        fh.write("project = %s\n" % (project))
        fh.write("create_date = %s\n" % (today))
        fh.write("clustering_method = unknown\n")
        fh.write("data_source = MatrixFile\n")
        fh.write("otu_size = \n")
        fh.write("sequence_count = \n")
        fh.write("otu_count = %s\n" % (str(otu_count)))
        if tax_available:
            fh.write("taxonomy_available = yes\n")
        else:
            fh.write("taxonomy_available = no\n")
        for i,ds in enumerate(dataset):
            fh.write("dataset[] = %s\n" % (ds))
        fh.close()
        
        
        
        #print dataset,dataset_count
        
        #print ds_counts
        
        load_db(cursor, dataset, otu, data_matrix, ds_counts, otu_taxonomy, project, user, run, otu_count, 'mtxfile')
        

    
def load_db(cursor, datasets, otus, matrix, ds_counts, taxonomy, project, user, run, otu_count, data_source):
    ds_cluster_count={}
    for n,otu_name in enumerate(otus):
            cluster = otu_name
            
            for m,ds_name in enumerate(datasets):
            
                if ds_name in ds_cluster_count:
                
                    if otu_name in ds_cluster_count[ds_name]:
                        ds_cluster_count[ds_name][otu_name] += 1
                    else:
                        ds_cluster_count[ds_name][otu_name] = 1
                else:                
                      
                    ds_cluster_count[ds_name] = {}
            
                #ds_cluster_count[ds_name]
                if ds_name in ds_counts:
                    dataset_count = ds_counts[ds_name]
                else:
                    dataset_count = 0
                knt     = matrix[n][m]
                if knt != 0 and knt != '0':  # do not put zeros in db
                    if dataset_count != 0 and dataset_count != '0':
                        frequency = float(knt)/dataset_count
                    else:
                        frequency = 0
                     
                    #print knt,dataset_count,frequency
                    tax = taxonomy[otu_name]
                    rank = ranks[len(tax.split(';'))]
                    project_dataset = project+'--'+ds_name
                    
                    query1 = "insert into otu_user_clusters ( \
                        upload_id,cluster,project,dataset,knt,dataset_count,frequency,taxonomy,rank,project_dataset,user,domain,otu_size \
                        ) values ( '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s' )" % \
                            (run,         cluster,    project,    ds_name,    knt,      
                            dataset_count,  frequency,  tax,        rank,       project_dataset,    
                            user,           'unknown',      'unknown'  
                            )
                    #print query1
                    try:
                        cursor.execute(query1)
                    except:
                        pass
    
    print 'rows '+str(n)
    sum_counts=0 
    dataset_names=[]
    for m,ds_name in enumerate(datasets):
        dataset_names.append(ds_name)
#         if ds_name in ds_counts:
#             dataset_count = ds_counts[ds_name]
#         else:
#             dataset_count = 0
#         sum_counts += dataset_count
#         print ds_name,ds_counts[ds_name]
        
        query2 = "insert into otu_projects_datasets (upload_id,project,dataset,dataset_info) \
                    values( '%s','%s','%s','%s')" % \
                    (run,project,ds_name,ds_name)                    
        try:
            cursor.execute(query2)
        except:
            pass
    if data_source=='mtxfile':
        description = 'From Matrix File';    
        title='From Matrix File'
    elif data_source=='biomfile':
        description = 'From Biom File';    
        title='From Biom File'
    else:
        description = '';    
        title=''
    
    
    query3 = "insert into otu_project_info (upload_id,project,title,description,user,upload_date,otu_size,domain,dna_region,method,otu_count) \
                    values( '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                    (run,project,title,description,user,today,'unknown','unknown','unknown','unknown',str(otu_count))
                    
    try:
        cursor.execute(query3)
    except:
        pass
  
if __name__ == '__main__':
    import argparse
    
    # DEFAULTS
    site = 'vampsdev'
    
    user = ''  
    
    data_object = {}
    
    

    
    myusage = """usage: otu_biom2db.py -ib biomFile  [options]
         
         Put user created otus into the vamps database. The OTUs must be in the
         form of a matrix file and a taxonomy file.
         
         where
            -ib, --biomfile The name of the matrix file.  [required]
            
            
            
            -m, --method     Has to be either usearch, crop or slp. 
                                [default: unknown]
            -s, --size       Percent similarity: generally 3, 6 or 10 percent
                                [default: unknown]
            -dom, --domain     Domain: Bacteria, Archaea or Eukarya 
                                [default: unknown]
            -reg, --region       DNA Region: v3, v6, v4v6, etc....
                                [default: unknown]                   
            -site            vamps or vampsdev.
                                [default: vampsdev]
            -id            upload_id.
                                [default: random number]
            -u, --user       Needed for otu naming and db access.
                             Will be retrieved from .dbconf if not supplied
            
    
    
    """
    parser = argparse.ArgumentParser(description="Upload Biom formatted files to the user_otus table." ,usage=myusage)
                                              
    
                                                        
    parser.add_argument("-site", "--db_host",      required=False,  action="store",   dest = "site", 
                                                    help="""database hostname: vamps or vampsdev
                                                        [default: vampsdev]""")  
    
    parser.add_argument("-u", "--user",         required=False,  action="store",   dest = "user", 
                                                    help="user name")  
                                              
    parser.add_argument("-r", "--run",         required=False,  action="store",   dest = "run", default='',
                                                    help="run number")  
                                                    
    parser.add_argument("-ib", "--biomfile",  required=False,  action="store",   dest = "biomfile", default='',
                                                    help="Biom File ") 
    parser.add_argument("-im", "--mtxfile",  required=False,  action="store",   dest = "mtxfile", default='',
                                                    help="Matrix File ")  
    parser.add_argument("-path", "--filepath",  required=True,  action="store",   dest = "filepath", 
                                                    help="Base File Path ")                                                  
    parser.add_argument('-v', '--verbose',  required=False,   action="store_true",   dest = "verbose",        
                                                 help = 'Turns on chatty output')                                                 
    #parser.add_argument("-q", "--quiet",    required=False, action="store_true",   dest="QUIET", 
    #                                             help="don't print any status messages to stdout")
    print "Starting otu_file2db.py"
    args = parser.parse_args()
    
    
    data_object['biomfile'] = args.biomfile
    data_object['mtxfile'] = args.mtxfile
    data_object['datetime'] = str(datetime.date.today())
    data_object['filepath'] = args.filepath 
    
    
    from_vamps = False
    
    data_object['from_vamps'] = from_vamps
    
    
    if args.site:
        site = args.site
    
    if site == 'vamps':
        db_host = 'vampsdb'
        db_name = 'vamps'
        db_home = '/groups/vampsweb/vamps/'
    else:
        db_host = 'vampsdev'
        db_name = 'vamps'
        db_home = '/groups/vampsweb/vampsdev/'
    
    
    obj=db.Conn(db_host, db_name, db_home)
    data_object['cursor'] = obj.get_cursor()
    if args.user:
        user = args.user
    else:
        user = obj.get_db_user()
    data_object['user'] = user
    
    if args.run:
        run = args.run
    else:
        import random
        run = str(random.randint(1000000,9999999))
    
    data_object['run'] = run
    if data_object['biomfile']:
        run_biom(data_object)
    else:
        run_matrix(data_object)
    