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
import IlluminaUtils.lib.fastalib as fastalib
import json
import datetime



def go(args):
    print(args)
    
    infile = args.infile
    
    #outfile = infile+'.OUT'
    
    #outfp = open(outfile,'w')  # or 'a'
    
    #fp = open(args.infile,'r')
    
    with open(infile) as json_file:
        data = json.load(json_file)
        for d in data:
            
            #print('\ndid '+d)
            
            for i in data[d]:
                if 'Simkus, Danielle N., et al. Variations in microbial' in str(data[d][i]):
                    print(d)
                    #print(' '+str(i)+'::'+str(data[d][i]))
    # n = 1
#     for line in fp.readlines():
#     #with open(infile) as fp:
#         
#         if n > 100:
#             break
#         if not line:
#             continue
#         else:
#             line = line.strip()  # clean off white space and LF
#             print(line)
#             ## do anything to your line
#             outfp.write(line+'\n')
#             n+=1
#     outfp.close()

if __name__ == '__main__':
    import argparse

    myusage = """usage: python_base_script.py [options]

         
         where
            -i, --infile The name of the input file.  [required]

        

    """
    parser = argparse.ArgumentParser(description="" ,usage=myusage)
    
    parser.add_argument('-i', '--infile',       required=True, action="store",   dest = "infile",   help = '')
    
    parser.add_argument("-t", "--upload_type",  required=False,  action="store",   dest = "type", help="raw or trimmed")
    parser.add_argument("-site",                required=False,  action="store",   dest = "site", help="""""")
    parser.add_argument("-r", "--runcode",      required=False,  action="store",   dest = "runcode",)
    parser.add_argument("-u", "--user",         required=False,  action="store",   dest = "user",  help="user name")
    parser.add_argument("-file_type",           required=False,  action="store",   dest = "file_type",     help="sff, fasta or fastq")
    parser.add_argument('-file_base',           required=False, action="store",   dest = "file_base",   help = 'where the files are loacated')


    if len(sys.argv[1:])==0:
        print(myusage)
        sys.exit()
    args = parser.parse_args()


    go(args)
