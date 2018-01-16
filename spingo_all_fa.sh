#! /bin/bash

db='/groups/vampsweb/seqinfobin/SPINGO/database/RDP_11.2.full_taxa.fa'
spingo_dir='SPINGO'
mkdir $spingo_dir
#echo "for file in *.fa.unique; do spingo -i $file -d $db > $file.spingo.out; done"
for file in *.fa.unique; 
    do 
    echo "spingo -i $file -d $db > $spingo_dir/$file.spingo.out"
    spingo -i $file -d $db > $spingo_dir/$file.spingo.out; 
done

#grendel
curr_path=`pwd`
curr_dir=${PWD##*/} 
#echo "Run on Grendel, change the reference file name accordingly:"
#echo "cd $curr_path; run_gast_ill_qiita_sge.sh -s $curr_dir -d gast -v -e fa.unique -r refssu -f -p both"

