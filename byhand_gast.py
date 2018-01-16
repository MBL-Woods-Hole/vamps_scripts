#!/usr/bin/env python

import os, sys
import subprocess

"""
source /groups/vampsweb/vamps/seqinfobin/vamps_environment.sh

cd /groups/vampsweb/vamps//tmp/mobedac_XXXXXX_gast

/groups/vampsweb/vamps/vamps_gast.py 
    -r 75814942 
    -p Asmith_test2 
    -u mobedac 
    -reg unknown 
    -site vamps 
    -dom unknown 
    -out /groups/vampsweb/vamps//tmp/mobedac_XXXXXX
    --use_cluster 
    --full_length  
    --classifier GAST


"""
def start_gast_byhand(args):
   
    if args.domain == 'unknown' or args.domain == '':
        use_full_length = True
        args.domain = 'unknown'
    
    vamps_gast_cmd = args.prefix+'vamps_gast.py'
    vamps_gast_cmd += ' -r ' + args.runcode
    vamps_gast_cmd += ' -p ' + args.project 
    vamps_gast_cmd += ' -u ' + args.user
    vamps_gast_cmd += ' -reg ' + args.dna_region
    vamps_gast_cmd += ' -site ' + args.site
    vamps_gast_cmd += ' -dom ' + args.domain
    vamps_gast_cmd += ' -out ' + args.outdir
    if use_full_length:
        vamps_gast_cmd += ' --full_length '
    if args.use_cluster:
        vamps_gast_cmd += ' --use_cluster '
    vamps_gast_cmd += ' --classifier GAST'
    if args.use64bit:
        vamps_gast_cmd += ' --use64bit'
    
    print vamps_gast_cmd
    proc = subprocess.Popen(vamps_gast_cmd, shell=True)


if __name__ == '__main__':
    import argparse
    
 #  /groups/vampsweb/vampsdev/vamps_gast.py -r 50970327 -p Single100 -u avoorhis -reg v6 -site vampsdev -dom bacteria 
    #     -out /groups/vampsweb/vampsdev/tmp/avoorhis_50970327 --use_cluster --classifier GAST --use64bit
    
    myusage = """usage: vamps_gast_by_hand.py  [options]
         
  Create new or Enter a directory in /groups/vampsweb/vamps(dev)/tmp/    
  Directory should have analysis/gast/(datasets)/seqfile.fasta structure
 ../../vamps_gast_byhand.py -out mobedac_XXXX -r XXXX -u mobedac  -p MBE_XXXX_Bv4 -site vamps -env 140 --cl     
         where   ALL REQUIRED
            -out            is the directory you are in (no path)   [-out mobedac_XXXX]
            -site           vamps or vampsdev                       [-site vamps]
            -r, --r         This is the center of your directory:   [-r 50970327]
            -u, --u         vamps user (also first part of dir)     [-u avoorhis]                        
            -p, --p         vamps project                           [-p MBE_XXXX_Bv4  ]    
            -reg, --reg     v3,v6, v4v6 ....
            -dom, --dom     bacteria, archaea, eukarya
            -cl, --cl       use the cluster [DEFAULT: True]
            --use64bit      use 64 bit usearch [DEFAULT: True]
    
    
    """
    #print myusage
    
    parser = argparse.ArgumentParser(description="" ,usage=myusage)                 
    
    
    parser.add_argument("-site","--site",       required=True,  action="store",   dest = "site", default='vampsdev',
                                                        help="""database hostname: vamps or vampsdev
                                                        [default: vampsdev]""") 
    parser.add_argument("-out", "--out",        required=True,  action="store",   dest = "output_dir", 
                                                        help = 'NO PATH')   
                                                        
    parser.add_argument("-r", "--r",            required=True,  action="store",   dest = "runcode", 
                                                        help="")  
    parser.add_argument("-u", "--u",            required=True,  action="store",   dest = "user", 
                                                        help="user name")         
    parser.add_argument("-p", "--p",            required=True,  action='store', dest = "project", 
                                                        help="")                                                 
    parser.add_argument('-reg',"--reg",         required=False, action="store",   dest = "dna_region", default='unknown',
                                                        help = 'v3, v6 ...') 
    parser.add_argument('-dom',"--dom",         required=False,  action="store",   dest = "domain", default='unknown',
                                                        help = 'archaea, bacteria, ...')                                           
    parser.add_argument('-use64bit',"--use64bit",   required=False,  action="store_true",   dest = "use64bit", default=True,
                                                        help = '')                                                                                                       
    parser.add_argument("-cl", "--cl",           required=False,  action="store_true",   dest = "use_cluster", default=True,
                                                        help = '')                                                                      
                                              
    #steps ='gast'
    #steps ='vampsupload'
    args = parser.parse_args()
    
    # use_cluster is set to true by default in the php code
    # here it can be set to False
    #args.use_cluster=False
    print myusage
    print """Did you run this command: 
    
    source /groups/vampsweb/vamps/seqinfobin/vamps_environment.sh
    
    
and are you in the correct directory?""",args.output_dir,"""

And with the proper permissions set:
cleanup gast directory
sudo -u vamps(dev)httpd chmod -R ug+rw ../()*
"""
    print "use_cluster and use64bit are turned on by default"
    print
    raw_input("Press Enter to continue...")

    
    args.prefix = '/groups/vampsweb/'+args.site+'/'
    args.outdir = args.prefix +'/tmp/'+ args.output_dir
    
    
    start_gast_byhand(args)
        
