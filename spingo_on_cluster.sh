#!/bin/bash

function verbose_log () {
    if [[ $verbosity -eq 1 ]]; then
        echo "$@"
    fi
}

#  ./spingo_on_cluster.sh -s STUDY_NAME -d gast -v -e fa.unique -r refssu -f -t 8 -p both (-i 1) 
# args
# DEFAULTS

spingo_dir="SPINGO"
# RUN_LANE=`date`
RUN_LANE=`echo "$[ 1 + $[ RANDOM % 1000 ]]"`
threads="0"

spingo_db_path='/groups/vampsweb/seqinfobin/SPINGO/database'
ref_db='RDP_11.2.full_taxa.fa'
verbosity=0
indir=`pwd`
file_ext='fa.unique'
USAGE="spingo_on_cluster.sh Optional arguments: [-d spingo output directory (default: $spingo_dir)] [-s project name] [-g path to spingo ref files (default: $spingo_db_path)] [-v verbosity (default: 0)] [-e processing filename extension (default: $file_ext)] [-r reference database filename (default: $ref_db)] [-h this statement]"

add_options='d:s:t:g:e:r:x:vh'
while getopts $add_options add_option
do
    case $add_option in
        d  )    spingo_dir=$OPTARG;;
        s  )    RUN_LANE=$OPTARG;;
        g  )    spingo_db_path=$OPTARG;;
        e  )    file_ext=$OPTARG;;
        r  )    ref_db=$OPTARG;;
        x  )    indir=$OPTARG;;
        v  )    verbosity=1;;
        h  )    echo $USAGE; exit;;
        \? )    if (( (err & ERROPTS) != ERROPTS ))
                then
                    error $NOEXIT $ERROPTS "Unknown option."
                fi;;
        *  )    error $NOEXIT $ERROARG "Missing option argument.";;
    esac
done

shift $(($OPTIND - 1))

printf "%s %d\n" "Verbosity level set to:" "$verbosity"

title="SPINGO on the vamps cluster."
# prompt="Please select a file name pattern:"
# options=("*.unique.nonchimeric.fa v4v5" "*.unique.nonchimeric.fa v4v5a for Archaea" "*.unique.nonchimeric.fa Euk v4" "*.unique.nonchimeric.fa Fungi ITS1" "*-PERFECT_reads.fa.unique" "*-PERFECT_reads.fa.unique for Archaea" "*MAX-MISMATCH-3.unique.nonchimeric.fa for Av6 mod (long)")

echo "$title"
FULL_OPTION=""
# NAME_PAT="*.unique.nonchimeric.fa";
# REF_DB_NAME=refssu;
REF_DB_NAME=$ref_db
#NAME_PAT=$indir'/'*$file_ext
NAME_PAT=*$file_ext

if [[ $full_ref -eq 1 ]]; then
    FULL_OPTION=" -full "
fi


verbose_log "spingo_dir = $spingo_dir"
verbose_log "RUN_LANE = $RUN_LANE"
verbose_log "spingo_db_path = $spingo_db_path"
#verbose_log "file_ext = $file_ext"
#verbose_log "ref_db = $ref_db"
#verbose_log "strand = $strand"


verbose_log "REF_DB_NAME = $ref_db"
#verbose_log "FULL_OPTION = $FULL_OPTION"
#verbose_log "IGNOREGAPS_OPTION = $IGNOREGAPS_OPTION"
verbose_log "NAME_PAT = $file_ext"

# gunzip first!
# gunzip *MERGED-MAX-MISMATCH-3.unique.nonchimeric.fa.gz
DIRECTORY_NAME=`pwd`
#db='/groups/vampsweb/seqinfobin/SPINGO/database/RDP_11.2.full_taxa.fa'
mkdir $spingo_dir
ls $NAME_PAT >$spingo_dir/filenames.list

cd $spingo_dir

FILE_NUMBER=`wc -l < filenames.list`
echo "total files = $FILE_NUMBER"

cat >spingo_on_cluster_$RUN_LANE.sh <<InputComesFromHERE
#!/bin/bash
#$ -cwd
#$ -S /bin/bash
#$ -N spingo_run
# Giving the name of the output log file
#$ -o spingo_on_cluster_$RUN_LANE.sh.sge_script.sh.log
# Combining output/error messages into one file
#$ -j y
# Send mail to these users
#$ -M avoorhis@mbl.edu
# Send mail; -m as sends on abort, suspend.
#$ -m as
#$ -t 1-$FILE_NUMBER
# Now the script will iterate $FILE_NUMBER times.
  
  source /groups/vampsweb/vamps/seqinfobin/vamps_environment.sh
 
  LISTFILE=./filenames.list
  INFILE=\`sed -n "\${SGE_TASK_ID}p" \$LISTFILE\`
  echo "====="
  echo "file name is \$INFILE"
  echo
 
  ## testing:
  
  echo "/groups/vampsweb/seqinfobin/spingo -i $DIRECTORY_NAME/\$INFILE -d $spingo_db_path/$REF_DB_NAME > $DIRECTORY_NAME/$spingo_dir/\$INFILE.spingo.out"
  /groups/vampsweb/seqinfobin/spingo -i $DIRECTORY_NAME/\$INFILE -d $spingo_db_path/$REF_DB_NAME > $DIRECTORY_NAME/$spingo_dir/\$INFILE.spingo.out; 
  #ls -l
  chmod 666 spingo_on_cluster_$RUN_LANE.sh.sge_script.sh.log
  
InputComesFromHERE

echo "Running spingo_on_cluster_$RUN_LANE.sh"
#qsub clust_gast_ill_$RUN_LANE.sh
# smp rotates through nodes in assingnment of tasks
# list parallel environments:  qconf -spl
# detail specific pe: qconf -sp smp
# check load on cluster hosts: qhost
# 24 slots here resulted in 2 jobs for each node on rotation (-pe smp)
# I've changed it to 8 slots: maybe get 6 jobs per node?
#qsub -pe smp clust_gast_ill_$RUN_LANE.sh
#qsub -l mem_free=12GB,h_vmem=13GB spingo_on_cluster_$RUN_LANE.sh
qsub spingo_on_cluster_$RUN_LANE.sh

