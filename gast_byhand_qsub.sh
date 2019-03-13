#!/bin/bash

# script to gast data which has been already loaded,separated and uniqued in a project directory
# by uploading fasta in NewVAMPS
# To run: copy this script to project_dir then edit -- change PROJECT, REFDB and check paths
# Next steps:
# /groups/vampsweb/vamps/nodejs/scripts/vamps_script_database_loader.py -site vamps -class GAST -project_dir pdir -config INFO.configPATH
# /groups/vampsweb/vamps/nodejs/scripts/vamps_script_upload_metadata.py -site vamps -project_dir pdir -p pname -config INFO.configPATH
# /groups/vampsweb/vamps/nodejs/scripts/vamps_script_create_json_dataset_files.py -site vamps -project_dir pdir -p pname -config INFO.configPATH --jsonfile_dir json
#
#
PROJECT=work
REFDB=refv4     
PDIR=/groups/vampsweb/vampsdev/nodejs/user_data/avoorhis/project-${PROJECT}
REFDB_PATH=/groups/g454/blastdbs/gast_distributions/${REFDB} 

ls $PDIR/analysis/*/seqfile.unique.fa > $PDIR/filenames.list
# chmod 666 $PDIR/filenames.list
cd $PDIR

 
FILE_NUMBER=`wc -l < $PDIR/filenames.list`
LISTFILE=$PDIR/filenames.list
echo "LISTFILE is $LISTFILE" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log
echo "LISTFILE is $LISTFILE"
echo \"total files = $FILE_NUMBER\" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log
echo \"total files = $FILE_NUMBER\"
cat >$PDIR/clust_gast_ill_${PROJECT}.sh <<InputComesFromHERE
    #!/bin/bash

    #$ -S /bin/bash
    #$ -N clust_gast_ill_${PROJECT}.sh
    # Giving the name of the output log file
    #$ -o $PDIR/cluster.log
    #$ -j y
    # Send mail to these users
    #$ -M avoorhis@mbl.edu
    # Send mail; -m as sends on abort, suspend.
    #$ -m as
    #$ -t 1-${FILE_NUMBER##*( )}
    # Now the script will iterate $FILE_NUMBER times.

    ##//  . /xraid/bioware/Modules/etc/profile.modules
    ##//  module load bioware

    source /groups//vampsweb/vampsdev/seqinfobin/vamps_environment.sh

    INFILE=\`sed -n "\${SGE_TASK_ID}p" $LISTFILE\`

    echo "=====" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log
    echo "file name is \$INFILE" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log
    echo "SGE_TASK_ID = \$SGE_TASK_ID" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log
    echo "/groups/vampsweb/seqinfobin/gast/gast_ill -saveuc -nodup -in \$INFILE -db ${REFDB_PATH}.fa -rtax ${REFDB_PATH}.tax -out \$INFILE.gast -uc \$INFILE.uc -threads 0" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log

    /groups/vampsweb/seqinfobin/gast/gast_ill -saveuc -nodup -in \$INFILE -db ${REFDB_PATH}.fa -rtax ${REFDB_PATH}.tax -out \$INFILE.gast -uc \$INFILE.uc -threads 0

    chmod 666 $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log

InputComesFromHERE

echo \"Running qsub -t 1-$FILE_NUMBER clust_gast_ill_${PROJECT}.sh\" >> $PDIR/clust_gast_ill_${PROJECT}.sh.sge_script.sh.log
echo \"Running qsub -t 1-$FILE_NUMBER clust_gast_ill_${PROJECT}.sh\"

##qsub -sync y $PDIR/clust_gast_ill_${PROJECT}.sh
qsub -t 1-$FILE_NUMBER -sync y $PDIR/clust_gast_ill_${PROJECT}.sh
 

 