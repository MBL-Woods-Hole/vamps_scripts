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

import os,stat
#from stat import * # ST_SIZE etc
import sys
import configparser as ConfigParser
from time import sleep
from os.path import expanduser
import datetime
import subprocess as subp
import gzip, csv, json
import pymysql as MySQLdb

sys.path.append('/groups/vampsweb/vampsdev')

# GLOBALS
allowed_ranks = ('domain', 'phylum', 'klass', 'order', 'family', 'genus', 'species', 'strain')


def run_new_fasta(args):
    print('run_new_fasta')
    print(args)
    # create out dir 
    working_dir = os.path.join(args.base, args.user +'-'+ args.runcode+ '_export-working-dir')
    working_dir_base_path = os.path.abspath(working_dir)
    os.mkdir(working_dir)
    NUMBERODIDS = len(args.dids)
    # did_list_file = os.path.join(working_dir_base_path,'did_list.txt')
#     fp_did = open(did_list_file,'w')
#     for did in args.dids:
#         fp_did.write(did+'\n')
#     fp_did.close()
    print('NUMBERODIDS')
    print(NUMBERODIDS)
    
    log_file_name   = os.path.join(working_dir_base_path,'export_log.log')
    if not os.path.exists(log_file_name):
        f = open(log_file_name, "w")
        f.close()
    #for i,did in enumerate(args.dids):
    #did='6105'
    #i=0
    #print(did)
    
    #script_filename = os.path.join(working_dir, 'script_' +str(did)+'-'+str(i+1)+'.sh')
    script_filename = os.path.join(working_dir, 'script.sh')
    #out_fasta_base       = os.path.join(working_dir_base_path, "fasta")
    qstat_name      = "Vexp"  #+str(i+1)
    fp = open(script_filename,'w')
    fp.write("#!/bin/sh\n\n")
    fp.write("source /groups/vampsweb/vampsdev/seqinfobin/vamps_environment.sh\n\n")
    fp.write(". /usr/share/Modules/init/sh\n")
    fp.write("export MODULEPATH=/usr/local/www/vamps/software/modulefiles\n")
    fp.write("module load clusters/vamps\n\n")
    fp.write("cd /groups/vampsweb/tmp\n\n")
    
    fp.write("function submit_job() {\n")
    fp.write("cat<<END | qsub -sync n \n")
    fp.write("\n")
    fp.write("\n")
    fp.write("\n")
    fp.write("#!/bin/bash\n")
    fp.write("#\$ -j y\n")
    fp.write("#\$ -o "+log_file_name+"\n")
    fp.write("#\$ -N "+qstat_name+"\n")
    fp.write("#\$ -cwd\n")
    fp.write("#\$ -t 1-"+str(NUMBERODIDS)+"\n")
    fp.write("#$ -V\n\n")
    fp.write("echo -n \"Hostname: \"\n")
    fp.write("hostname\n")
    fp.write("echo -n \"qsub: Current working directory: \"\n")
    fp.write("pwd\n\n")
    fp.write("source /groups/vampsweb/vampsdev/seqinfobin/vamps_environment.sh\n\n")
    
    
    fp.write("DIDARRY=("+' '.join(args.dids)+")\n")  
    fp.write("IDX=\$(( \$SGE_TASK_ID - 1 ))\n")
    fp.write("DID=\${DIDARRY[\$IDX]}\n")
    
    
    # SAVEME:: fp.write("DID=\`sed -n \"\${SGE_TASK_ID}p\" "+ did_list_file + "\` \n\n")
    fp.write("LOG="+working_dir_base_path+"/log-\${DID}.log\n")
    fp.write("echo IDX=\$IDX >> \${LOG}\n")
    fp.write("echo SGE_TASK_ID=\${SGE_TASK_ID} >> \${LOG}\n")
    fp.write("echo DID=\${DID} >> \${LOG}\n")
    fp.write("echo JOB_ID=\${JOB_ID} >> \${LOG}\n")
    fp.write("OUTFASTA=" + working_dir_base_path + "/\${DID}.fa" )
    
    fp.write("\n\n")
    fp.write("echo OUTFASTA=\${OUTFASTA} >> \${LOG}\n")
    fasta_cmd = "/groups/vampsweb/new_vamps_maintenance_scripts/db2fasta_per_did.py -host "+args.host+ " -d \${DID} -o \${OUTFASTA}"
    # select * from 
    fp.write(fasta_cmd)  
    
    fp.write("\n\n")
    fp.write("END\n")
    fp.write("}\n")
    fp.write("submit_job\n")    
    fp.write("\n\n")
    fp.close()
    os.chmod(script_filename, 0o775 )
    proc = subp.Popen(script_filename)
    #print('proc='+str(proc.pid))
        
if __name__ == '__main__':

    import argparse


    myusage = """usage: vamps_export_file.py  [options]


         where

            -host, --host       vamps or [default: vampsdev]

            -r,   --runcode

            -u, --user       Needed access code creation

            --file_base         Where the files will go and where is the INFO file
            --normalization     user choice: not_normalized, normalized_to_maximum or normalized_by_percent
            --compress          Compress files in gzip format

            --rank              used only for taxbytax, biom and matrix  [ DEFAULT:phylum ]
            --domains           [ DEFAULT:"Archaea, Bacteria, Eukarya, Organelle, Unknown" ]

            --taxbytax_file     if present will create TaxByTax file
            --taxbyref_file     if present will create TaxByRef file  NOT YET WORKING
            --taxbyseq_file     if present will create TaxBySeq file
            --fasta_file        if present will create Fasta file
            --matrix_file       if present will create Count Matrix file
            --biom_file         if present will create Biom file



    """
    parser = argparse.ArgumentParser(description = "", usage = myusage)



    parser.add_argument("-host", "--host",               required=True,  action="store",   dest = "host",
                                                    help="""database hostname: vamps or vampsdev or local(host)
                                                        [default: vampsdev]""")
    parser.add_argument("-r", "--runcode",      required=True,  action="store",   dest = "runcode",
                                                    help="like 12345678")
    parser.add_argument("-u", "--user",         required=True,  action="store",   dest = "user",
                                                    help="VAMPS user name")
    parser.add_argument("-dids", "--dids",         required=False,  action="store",   dest = "dids",default = '',
                                                    help="dataset_ids")
    parser.add_argument("-pids", "--pids",         required=False,  action="store",   dest = "pids", default = '',
                                                    help="project_ids")
    parser.add_argument("-c", "--cluster_available", required=False,  action="store_true",   dest = "cluster_available", default = False,
                                                    help="")
    parser.add_argument("-base", "--file_base",      required=True,  action="store",   dest = "base", help="Path without user or file")

    
    parser.add_argument("-db", "--db",
                required=False,  action='store', dest = "NODE_DATABASE",  default='vamps_development',
                help="NODE_DATABASE")
    parser.add_argument("-mg", "--include_metagenomic",
                required=False,  action='store_true', dest = "include_metagenomic",  default=False,
                help="")
    args = parser.parse_args()

    args.today = str(datetime.date.today())

    if args.host == 'vamps':
        db_host = 'vampsdb'
        #db_host = 'bpcweb8'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vamps/'
        args.files_home ='/groups/vampsweb/vamps/nodejs/json/vamps2--datasets_silva119/'
    elif args.host == 'vampsdev':
        db_host = 'bpcweb7'
        #db_host = 'bpcweb7'
        args.NODE_DATABASE = 'vamps2'
        db_home = '/groups/vampsweb/vampsdev/'
        args.files_home ='/groups/vampsweb/vampsdev/nodejs/json/vamps2--datasets_silva119/'
    else:
        db_host = 'localhost'
        db_home = '~/'
        args.files_home ='public/json/vamps_development--datasets_silva119/'
    db_name = args.NODE_DATABASE


    print (db_host, db_name)

    home = expanduser("~")
    print(home)
    args.obj = MySQLdb.connect( host=db_host, db=db_name, read_default_file=home+'/.my.cnf_node', cursorclass=MySQLdb.cursors.DictCursor    )

    output_dir = args.base
    
    
    args.dids = args.dids.strip('"').split(',')
    #args.pids = args.pids.strip('"').split(',')
    
    
    args.dids = [x.strip() for x in args.dids]
    #args.pids = [x.strip() for x in args.pids]
    
    run_new_fasta(args)
    print('Finished')

