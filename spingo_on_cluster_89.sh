#!/bin/bash
#$ -cwd
#$ -S /bin/bash
#$ -N spingo_run
# Giving the name of the output log file
#$ -o spingo_on_cluster_89.sh.sge_script.sh.log
# Combining output/error messages into one file
#$ -j y
# Send mail to these users
#$ -M avoorhis@mbl.edu
# Send mail; -m as sends on abort, suspend.
#$ -m as
#$ -t 1-0
# Now the script will iterate 0 times.
  
  source /groups/vampsweb/vamps/seqinfobin/vamps_environment.sh
 
  LISTFILE=./filenames.list
  INFILE=`sed -n "${SGE_TASK_ID}p" $LISTFILE`
  echo "====="
  echo "file name is $INFILE"
  echo
 
  ## testing:
  
  echo "/groups/vampsweb/seqinfobin/spingo -i /groups/vampsweb/new_vamps_maintenance_scripts/$INFILE -d /groups/vampsweb/seqinfobin/SPINGO/database/RDP_11.2.full_taxa.fa > /groups/vampsweb/new_vamps_maintenance_scripts/SPINGO/$INFILE.spingo.out"
  /groups/vampsweb/seqinfobin/spingo -i /groups/vampsweb/new_vamps_maintenance_scripts/$INFILE -d /groups/vampsweb/seqinfobin/SPINGO/database/RDP_11.2.full_taxa.fa > /groups/vampsweb/new_vamps_maintenance_scripts/SPINGO/$INFILE.spingo.out; 
  #ls -l
  chmod 666 spingo_on_cluster_89.sh.sge_script.sh.log
  
