#!/usr/bin/env python

import subprocess
import sys, os,stat,glob
import csv
import time
import shutil
import datetime
import argparse
import pymysql as MySQLdb
today = str(datetime.date.today()) 
#import ConMySQL
ranks = ('domain','phylum','class','orderx','family','genus','species','strain')
domains = ('Archaea','Bacteria','Eukarya','Organelle','Unknown')

class FastaReader:
    def __init__(self,file_name=None):
        self.file_name = file_name
        self.h = open(self.file_name)
        self.seq = ''
        self.id = None

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
            
def run_taxonomy(args, ds, ds_count, files):
    print('tax')
    project_dataset = args.project+'--'+ds
    taxa_lookup = {}
    read_id_lookup={}
    with open(files['gast_file'],'r') as f:
        next(f)  # skip header
        for line in f:
        
            line = line.strip()
            items = line.split("\t")
            if args.verbose:
                print(items)
            taxa = items[1]
            if taxa[-3:] == ';NA':
                taxa = taxa[:-3]
            #read_id=items[0]  # this is the whole id from mothur -> won't match the clean id 
            if args.datasetname_not_in_unique_file:
                read_id = items[0].split()[0]  #M00270:130:000000000-A8DLT:1:1101:21493:2094;barcodelabel=oranje-touw-zeeb1-2_S79|frequency:2174
                freq_from_defline = items[0].split(':')[-1]
            else:
                read_id = items[0].split()[1]  #10445.7p2_22437835 IIU3AEM07H62IP orig_bc=ACGAGTGCGT new_bc=ACGAGTGCGT bc_diffs=0|frequency:1
                freq_from_defline = items[0].split()[-1].split(':')[1]
            if args.verbose:
                print('id=',read_id)
                print('  freq=',freq_from_defline)
            read_id_lookup[read_id] = taxa
        
            # the count here is the frequency of the taxon in the datasets
            
            if taxa in taxa_lookup:                
                try:
                    taxa_lookup[taxa] += int(freq_from_defline)
                except:
                    taxa_lookup[taxa] += 1
            else:
                try:
                    taxa_lookup[taxa] = int(freq_from_defline)
                except:
                    taxa_lookup[taxa] = 1
        
    ###############################
    #  DATA CUBE TABLE
    # taxa_lookup: {'Unknown': 146, 'Bacteria': 11888, 'Bacteria;Chloroflexi': 101}
    # dataset_count is 3 (3 taxa in this dataset)
    # frequency is 3/144
    print('Running data_cube')
    fh1 = open(files['taxes_file'],'w')

    fh1.write("\t".join( ["HEADER","project", "dataset", "taxonomy", "superkingdom", 
                        "phylum", "class", "orderx", "family", "genus", "species", 
                        "strain", "rank", "knt", "frequency", "dataset_count", "classifier"]) + "\n")
    tax_collector={}
    summer=0
    for tax,knt in taxa_lookup.iteritems():
        if args.verbose:
            print(tax,knt)
        summer += knt
        datarow = ['',args.project,ds]
    
        taxa = tax.split(';')
        #if taxa[0] in C.domains:
        freq = float(knt) / int(ds_count)
        rank = ranks[len(taxa)-1]
        for i in range(len(ranks)):                
            if len(taxa) <= i:
                if ranks[i] == 'orderx':
                    taxa.append("order_NA")
                else:
                    taxa.append(ranks[i] + "_NA")

        tax_collector[tax] = {}


        datarow.append(tax)
        datarow.append("\t".join(taxa))
        datarow.append(rank)
        datarow.append(str(knt))
        datarow.append(str(freq))
        datarow.append(str(ds_count))
        datarow.append('GAST')
    
        w = "\t".join(datarow)
        
        fh1.write(w+"\n")
   
        tax_collector[tax]['rank'] = rank
        tax_collector[tax]['knt'] = knt
        tax_collector[tax]['freq'] = freq
        
    fh1.close()
    
    ########################################
    #
    # SUMMED DATA CUBE TABLE
    #
    ########################################
    print('Running summed_(junk)_data_cube')
    fh2 = open(files['summed_taxes_file'],'w')
    
    fh2.write("\t".join(["HEADER","taxonomy", "sum_tax_counts", "frequency", "dataset_count","rank", 
                        "project","dataset","project--dataset","classifier"] )+"\n")
    ranks_subarray = []
    rank_list_lookup = {}
    for i in range(0, len(ranks)): 
        ranks_subarray.append(ranks[i])
        ranks_list = ";".join(ranks_subarray) # i.e., superkingdom, phylum, class
        # open data_cube file again
        # taxes_file: data_cube_uploads
        for line in  open(files['taxes_file'],'r'):
            line = line.strip().split("\t")
            knt = line[12]
            taxon = line[2]
            if line[0] == 'HEADER':
                continue
            if taxon in tax_collector:
                knt = tax_collector[taxon]['knt']
            else:
                print('ERROR tax not found in tax_collector: assigning zero')
                knt = 0
            idx = len(ranks_subarray)
            l=[]
            for k in range(3,idx+3):                    
                l.append(line[k])
            tax = ';'.join(l)
           
            
            
            if tax in rank_list_lookup:
                rank_list_lookup[tax] += knt
            else:
                rank_list_lookup[tax] = knt
                
            
      
    for tax,knt in rank_list_lookup.iteritems():
        
        
        taxa = tax.split(';')
        #if taxa[0] in C.domains:
        rank = len( taxa ) -1
        
        frequency = float(knt) / int(ds_count)
        
        if len(tax) - len(''.join(taxa)) >= rank:
        
            datarow = ['']
            datarow.append(tax)
            datarow.append(str(knt))
            datarow.append(str(frequency))
            datarow.append(str(ds_count))
            datarow.append(str(rank))
            datarow.append(args.project)
            datarow.append(ds)
            datarow.append(project_dataset)
            datarow.append('GAST')
        
            w = "\t".join(datarow)
            
            fh2.write(w+"\n")
            

    fh2.close()
    
    
    ####################################       
    #
    # DISTINCT TAXONOMY
    #
    ####################################
    print('Running taxonomy')
    fh3 = open(files['distinct_taxes_file'],'w')
    fh3.write("\t".join(["HEADER","taxon_string", "rank", "num_kids"] )+"\n")
    taxon_string_lookup={}
    for line in  open(files['summed_taxes_file'],'r'):
        if line.split("\t")[0] == 'HEADER':
            continue
        items = line.strip().split("\t")            
        taxon_string = items[0]
       
        if taxon_string in taxon_string_lookup:
            taxon_string_lookup[taxon_string] += 1
        else:
            taxon_string_lookup[taxon_string] = 1
    
    for taxon_string,v in taxon_string_lookup.iteritems():
        datarow = ['']
        datarow.append(taxon_string)
        taxa = taxon_string.split(';')
        if taxa[0] in domains:
            rank = str(len(taxa)-1)
            datarow.append(rank)
            if rank==7 or taxon_string[-3:]=='_NA':
                num_kids = '0'
            else:
                num_kids = '1'
            datarow.append(num_kids)
            w = "\t".join(datarow)
            
            fh3.write(w+"\n")
    fh3.close()
    
    return (tax_collector,read_id_lookup)
    
def run_sequences(args, ds, tax_collector, read_id_lookup, files):
    print('Running sequences')
    refid_collector={}
    project_dataset = args.project+'--'+ds
    with open(files['gast_file'],'r') as f:
        next(f)  # skip header
        for line in f:
            line = line.strip()
        
            items=line.split("\t")
            if args.verbose:
                print(items)
            if args.datasetname_not_in_unique_file:
                id = items[0]
            else:
                id = items[1]
            distance = items[2]
            refhvr_ids = items[-1] # always last? separated by ,,
            if args.verbose:
                print('refhvr_ids',refhvr_ids)
            refid_collector[id] = {}
            refid_collector[id]['distance'] = distance
            refid_collector[id]['refhvr_ids'] = refhvr_ids
    fh = open(files['sequences_file'],'w')
    fh.write("\t".join(["HEADER","sequence","project","dataset","taxonomy","refhvr_ids", "rank",
                            "seq_count","frequency","distance","read_id","project_dataset"] )+"\n")
        
           
    # open uniques fa file
    f = FastaReader(files['unique_file'])
    while f.next():
        datarow = ['']
        defline_items = f.id.split()
        if args.datasetname_not_in_unique_file:
            id = defline_items[0]
        else:
            id = defline_items[1]
                       
        cnt = defline_items[-1].split(':')[-1]
        if args.verbose:
            print('cnt from uniques file',cnt)
        seq = f.seq.upper()
        if id in read_id_lookup:
            if args.verbose:
                print('FOUND TAX for sequences file')
            tax = read_id_lookup[id]
        else: 
            print('ERROR:: NO TAX for sequences file')
            tax = ''
            
        if tax in tax_collector:
            rank = tax_collector[tax]['rank']
            #cnt = tax_collector[tax]['knt']
            freq = tax_collector[tax]['freq']
        else:
            rank = 'NA'
            cnt  = 0
            freq = 0
            
        if id in refid_collector:
            distance = refid_collector[id]['distance']
            refhvr_ids = refid_collector[id]['refhvr_ids']
        else:
            distance = '1.0'
            refhvr_ids = '0'
        if not cnt:
            cnt = 1
        datarow.append(seq)
        datarow.append(args.project)
        datarow.append(ds)
        datarow.append(tax)
        datarow.append(refhvr_ids)
        datarow.append(rank)
        datarow.append(str(cnt))
        datarow.append(str(freq))
        datarow.append(distance)
        datarow.append(id)
        datarow.append(project_dataset)
        w = "\t".join(datarow)
       
        fh.write(w+"\n")
    fh.close()
    return refid_collector      
    
            
def run_projects(args, ds, ds_count, files):
    print('Running projects')
    project_dataset = args.project+'--'+ds
    date_trimmed = 'unknown'
    dataset_description = ds
    has_tax = '1' # true
    fh = open(files['projects_datasets_file'],'w')
    
    fh.write("\t".join(["HEADER","project","dataset","dataset_count","has_tax", "date_trimmed","dataset_info"] )+"\n")
    fh.write("\t"+"\t".join([args.project, ds, str(ds_count), has_tax, date_trimmed, dataset_description] )+"\n")
    
    fh.close()
    
def run_info(args, ds, files, project_count):
    print('Running info')
    
    try:
        # get 'real' data from database
        db = get_db_connection(args)
        cursor = db.cursor()
        print('Connecting to '+args.site+' database, to get user "'+args.user+'" information')
        query = "SELECT last_name,first_name,email,institution from vamps_auth where user='%s'" % (args.user)
        print(query)
        
        cursor.execute(query)
        data = cursor.fetchone()
        contact= data[1]+' '+data[0]
        email= data[2]
        institution= data[3]
    except:
        print('TESTING -- no writing to '+args.site+' db')
        contact= 'test,test'
        email= 'test@no-reply.edu'
        institution= 'TEST U.'
    
    fh = open(files['project_info_file'],'w')
    title="title"
    description='description'
    
    
    fh.write("\t".join(["HEADER","project","title","description","contact", "email","institution","user","env_source_id","edits","upload_date","upload_function","has_tax","seq_count","project_source","public"] )+"\n")
    fh.write("\t"+"\t".join([args.project, title, description, contact, email, institution, args.user, args.env_source_id, args.user, today, 'script', '1', str(project_count), 'script', '0'] )+"\n")
    # if this project already exists in the db???
    # the next step should update the table rather than add new to the db
    
    fh.close()
    

def get_db_connection(args):
    db = MySQLdb.connect( host=args.site, db=args.database,
             read_default_file="~/.my.cnf" # you can use another ini file, for example .my.cnf
           )
    cur = db.cursor()

    return db

def create_vamps_files(args, ds, ds_count, project_count):
    """
    Creates the vamps files in the gast directory ready for loading into vampsdb
    """
    files = gather_files_per_ds(args, ds)
    if ds_count:
         (tax_collector, read_id_lookup) = run_taxonomy(args, ds, ds_count, files)
         refid_collector = run_sequences(args, ds, tax_collector, read_id_lookup, files)   
         run_projects(args, ds, ds_count, files)
         run_info(args, ds, files, project_count)
    else:
        print("no tagtax file found or no dataset_count -- continuing to next dataset..." )
    return files
           
def gather_files_per_ds(args, ds):
    files={}
    files['gast_file']                 = os.path.join(args.indir,ds+'.fa.unique.gast')
    files['unique_file']               = os.path.join('./',ds+'.fa.unique')
    files['names_file']                = os.path.join('./',ds+'.fa.names')
    # to be created:
    files['taxes_file']                = os.path.join(args.outdir,ds,'vamps_data_cube_uploads.txt')
    files['summed_taxes_file']         = os.path.join(args.outdir,ds,'vamps_junk_data_cube_pipe.txt')
    files['distinct_taxes_file']       = os.path.join(args.outdir,ds,'vamps_taxonomy_pipe.txt')
    files['sequences_file']            = os.path.join(args.outdir,ds,'vamps_sequences_pipe.txt')
    files['projects_datasets_file']    = os.path.join(args.outdir,ds,'vamps_projects_datasets_pipe.txt')
    files['project_info_file']         = os.path.join(args.outdir,ds,'vamps_upload_info.txt')
    
    return files

def get_datasets(args):
    
    ds_list = {}
    project_count = 0
    files = glob.glob(os.path.join('./',"*.fa.unique"))
    
    for file in files:
        ds_count = 0
        base = os.path.basename(file)
        ds = base[:-10]
        if args.verbose:
            print('ds',ds)
        r = FastaReader(file)
        while r.next():
            defline = r.id
            parts = defline.split()
            # for MoBE:  ['10445.7p2_22423662', 'IIU3AEM07H2G36', 'orig_bc=ACGAGTGCGT', 'new_bc=ACGAGTGCGT', 'bc_diffs=0|frequency:1']
            if args.datasetname_not_in_unique_file:
                true_id = parts[0]
            else:
                true_id = parts[1]
            cnt = parts[-1].split(':')[1]
            
            ds_count += int(cnt)
            seq = r.seq
        project_count += int(ds_count)
        ds_list[ds]=ds_count
    print(ds_list)
    return (ds_list, project_count)
    

def start_vamps_file_creation(args, ds_list, project_count):
    
    # Question for anna about counts: the names file will have just one entry (2 columns) IF the
    # original seqs file was already uniqued BUT there will be a 'frequency:X' tagged on the end of the defline
    # so two ways to get counts: 1-from frequency:X tag ( may not be present)
    # OR 2-Count columns in names file
    # Answer check defline: if frequency:X is present use it -- if not: use names file
    file_collector={}
    args.outdir = create_out_dir(args, ds_list)
    for i,ds in enumerate(ds_list):
        print()
        
        if ds_list[ds] >= int(args.min_ds_count):
            print('CreatingFiles',args.project,ds,i,'/',len(ds_list))
            check_for_infiles(args, ds)
            file_collector[ds] = create_vamps_files(args, ds, ds_list[ds], project_count)
        else:
            print('Skipping',ds,'count is less than',args.min_ds_count)
    
    return file_collector
    
def check_for_infiles(args,ds):
    names = os.path.join('./',ds+'.fa.names')
    gast  = os.path.join(args.indir,ds+'.fa.unique.gast')
    unique= os.path.join('./',ds+'.fa.unique')
    # grab csv file:                10445.7p2.fa.unique.gast
    # grab sequences file:          10445.7p2.fa.unique
    # grab names file for counts:   10445.7p2.fa.names
    if not os.path.isfile(names):
        sys.exit( 'no names file found--exiting')
    if not os.path.isfile(gast):
        sys.exit( 'no gast file found--exiting')
    if not os.path.isfile(unique):
        sys.exit( 'no unique file found--exiting')
    print('all files found for '+ds+'*')
    

    
def create_out_dir(args, ds_list):
    outdir = os.path.join(args.outdir_base,args.project)
    if os.path.exists(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir)
    for ds in ds_list:
        if ds_list[ds] >= int(args.min_ds_count):
            os.makedirs(os.path.join(outdir,ds))
    return outdir
    

            

if __name__ == '__main__':
    
    
    # DEFAULTS
    site = 'vampsdev'
    
    

    data_object = {}
    

    
    myusage = """usage: 4-vamps_classic_create_vamps_files.py  [options]
         
         Pipeline: /groups/vampsweb/vamps/maintenance_scripts/*
            1-demultiplex and unique (using multi dataset *fna file 'qiita' style)
	            1-qiita_fna_process.sh FILE_NAME.fna  (dataset unique files appear in current directory)
                If single dataset - do this step by hand: fastaunique
            2- GAST 
                2-vamps_gast_byhand_qiita.sh -s STUDY_NAME -d gast -v -e fa.unique -r refssu -f -p both
            3- check fasta files and percentages (run in gast directory)
                3-percent10_gast_unknowns.sh  (no flags)
            4- create vamps files  (run in same directory as gast directory)
                4-vamps_classic_create_vamps_files.py -s vamps -p PROJECT_NAME
            5- load vamps files into vampsdb database  (run in same directory as gast directory)
                5-vamps_classic_load_gast_from_files.py -s vamps -p PROJECT_NAME -i PROJECT_NAME

         
         where
            
            Current dir contains the *names and *unique files
            While the gast files are in the ./gast directory
            
            -s/--site     vamps or [vampsdev]        
            
            -i/--indir    Default: 'gast' in current dir     
            
            -p/--project  project name: REQUIRED     
            
            
            -o/--outdir   Where vamps files will be created: defaults to [project-name] which will be created. 
            
            
            
            Others to consider:
            -user
            
            -env_source
            
            -v/--v verbose
            
    
    
    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)                 
    
# -rw-r--r-- 1 avoorhis avoorhis  103258 Apr 15 11:21 10445.6d10.fa.names
# -rw-r--r-- 1 avoorhis avoorhis  239658 Apr 15 11:21 10445.6d10.fa.unique
# -rw-r--r-- 1 avoorhis avoorhis  151503 Apr 15 11:21 10445.6d10.fa.unique.gast
# -rw-r--r-- 1 avoorhis avoorhis    6586 Apr 15 11:21 10445.6d2.fa.names
# -rw-r--r-- 1 avoorhis avoorhis   15323 Apr 15 11:21 10445.6d2.fa.unique
# -rw-r--r-- 1 avoorhis avoorhis   10145 Apr 15 11:21 10445.6d2.fa.unique.gast
# -rw-r--r-- 1 avoorhis avoorhis  198632 Apr 15 11:21 10445.7p2.fa.names
# -rw-r--r-- 1 avoorhis avoorhis  463718 Apr 15 11:21 10445.7p2.fa.unique
# -rw-r--r-- 1 avoorhis avoorhis  307694 Apr 15 11:21 10445.7p2.fa.unique.gast

    
                                                     
    parser.add_argument("-s", "--site",          required=False,  action="store",   dest = "site", default='vampsdev',
                                                        help="""database hostname: vamps or vampsdev  [default: vampsdev]""")    
    parser.add_argument("-i",   "--indir", required=False,  action="store",   dest = "indir", default='./gast',   help="Where dataset dirs are")       
    parser.add_argument("-p", "--project",    required=False,  action='store', dest = "project",  help="")   
    parser.add_argument("-o",     "--outdir",     required=False,  action='store', dest = "outdir_base",  default='./',help="")  
    parser.add_argument("-min",     "--min_ds_count",     required=False,  action='store', dest = "min_ds_count",  default='10',help="")                                        
    # others
    parser.add_argument("-v","--v",  required=False,  action="store_true",   dest = "verbose", default=False)  
    
    parser.add_argument("-u",        "--user",     required=False,  action="store",   dest = "user", default='admin')  
    # '100' is unknown                                   
    parser.add_argument("-env",      "--env_source_id",  required=False,  action="store",   dest = "env_source_id", default='100')  
    parser.add_argument("-datasetname_not_in_unique_file",      "--datasetname_not_in_unique_file",  required=False,  action="store_true",   
                        dest = "datasetname_not_in_unique_file", default=False) 
    parser.add_argument("-otu",      "--otu_table",  required=True,  action="store",   
                        dest = "otu_table", default=False)                                                                                                                                                           
    if len(sys.argv[1:]) == 0:
        print(myusage)
        sys.exit() 
    args = parser.parse_args()
    
    if args.site == 'vamps':
        args.site = 'vampsdb'
    args.database = 'vamps'
    file_collector = {}
    (ds_list, project_count) = get_datasets(args)
    
        
    file_collector = start_vamps_file_creation(args, ds_list, project_count)
    
    
    
        
    