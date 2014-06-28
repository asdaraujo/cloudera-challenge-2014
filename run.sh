#!/bin/bash
# 
# This script runs the steps to answer the Cloudera Data Science Challenge 2 (2014).
# It expects all the input files to be already loaded in HDFS. The first parameter
# for this script is the parent directory for all the data (INPUT_DIR). This directory
# must have the following subdirs:
# 
#  - inpatient - contains the zipped CSV file with the Inpatient services summary data
#  - outpatient - contains the zipped CSV file with the Outpatient services summary data
#  - patient - contains the XML file with the patient records (unzipped)
#  - patientclaim - contains the ADT files with the patient claim data (unzipped)
#  - review - contain the REVIEW.TXT file with the list of suspicious records for review
#  
# All the files above were provided by the challenge. The full listing of files is shown
# below:
# 
#  $INPUT_DIR/inpatient/Medicare_Provider_Charge_Inpatient_DRG100_FY2011.csv.gz
#  $INPUT_DIR/outpatient/Medicare_Provider_Charge_Outpatient_APC30_CY2011_v2.csv.gz
#  $INPUT_DIR/patient/PNTSDUMP.XML
#  $INPUT_DIR/patientclaim/PCDR1101.ADT
#  $INPUT_DIR/patientclaim/PCDR1102.ADT
#  $INPUT_DIR/patientclaim/PCDR1103.ADT
#  $INPUT_DIR/patientclaim/PCDR1104.ADT
#  $INPUT_DIR/patientclaim/PCDR1105.ADT
#  $INPUT_DIR/patientclaim/PCDR1106.ADT
#  $INPUT_DIR/patientclaim/PCDR1107.ADT
#  $INPUT_DIR/patientclaim/PCDR1108.ADT
#  $INPUT_DIR/patientclaim/PCDR1109.ADT
#  $INPUT_DIR/patientclaim/PCDR1110.ADT
#  $INPUT_DIR/patientclaim/PCDR1111.ADT
#  $INPUT_DIR/patientclaim/PCDR1112.ADT
#  $INPUT_DIR/review/REVIEW.TXT
# 
# The only file name that must match the above is the "REVIEW.TXT" file. The others
# can be named anything as long as the directory is specified as above.
#
# The full syntax for running this script is:
# 
#   ./run.sh <hive_db_name> <input_dir> <output_dir> <schema_dir> <results_dir>
#   
#     hive_db_name - The name of the Hive database used to create needed tables.
#                    The database must already exist.
#                    Default: $USER
#                    
#     input_dir - The HDFS directory containing the input files, as explained above.
#                 Default: hdfs:///user/$USER/ccinput
# 
#     output_dir - The HDFS directory used for processed output.
#                  Default: hdfs:///user/$USER/ccoutput
# 
#     schema_dir - The HDFS directory used to store Avro schema files used in the data
#                  preparation.
#                  Default: hdfs:///user/$USER/ccschema
#                  
#     results_dir - The LOCAL directory used to create the deliverable files required
#                   by the challenge instructions
#                   Default: ./results
#                   

# Globals
readonly DB_NAME=${1:-$USER}
echo "Hive db name: $DB_NAME"
readonly INPUT_DIR=${2:-hdfs://$( hdfs getconf -nnRpcAddresses )/user/$USER/ccinput}
echo "Input dir:    $INPUT_DIR"
readonly OUTPUT_DIR=${2:-hdfs://$( hdfs getconf -nnRpcAddresses )/user/$USER/ccoutput}
echo "Output dir:   $OUTPUT_DIR"
readonly SCHEMA_DIR=${3:-hdfs://$( hdfs getconf -nnRpcAddresses )/user/$USER/ccschema}
echo "Schema dir:   $SCHEMA_DIR"
readonly RESULTS_DIR=${4:-results}
echo "Results dir:  $RESULTS_DIR"
echo ""

readonly JAR_FILE=$( ls -1tr target/araujo-ccds2-*-jar-with-dependencies.jar | tail -1 )
if [ "$JAR_FILE" == "" ]; then
    echo "ERROR: Can't find the project jar file. Please run build.sh before running this script"
    exit 1
fi

function h1() {
    local msg="$1"
    printf "\n************************************************************\n"
    printf "*                                                          *\n"
    printf "*  %-54s  *\n" "$msg"
    printf "*                                                          *\n"
    printf "************************************************************\n"
}

function h2() {
    local msg="$1"
    printf "\n## %s\n" "$msg"
}

function data_preparation() {
    h1 "Data preparation"

    h2 "Create output dir"
    hdfs dfs -stat $OUTPUT_DIR > /dev/null 2>&1
    if [ $? == 0 ]; then
        hdfs dfs -mv $OUTPUT_DIR $OUTPUT_DIR.$( date +%Y%m%d%H%M%S )
    fi
    hdfs dfs -mkdir -p $OUTPUT_DIR
    
    h2 "Run crunch job to process all the different input formats and write the data into SequenceFiles"
    time hadoop jar $JAR_FILE \
        com.asdaraujo.InputProcessJob \
        $INPUT_DIR/patientclaim \
        $INPUT_DIR/patient \
        $INPUT_DIR/inpatient \
        $INPUT_DIR/outpatient \
        $OUTPUT_DIR/patientclaim \
        $OUTPUT_DIR/patient \
        $OUTPUT_DIR/inpatient \
        $OUTPUT_DIR/outpatient
    
    h2 "Create Hive tables to access the data"
    hive -v -f scripts/setupHive.hql \
        --database $DB_NAME \
        -hivevar outputdir=$OUTPUT_DIR \
        -hivevar schemadir=$SCHEMA_DIR
    
    h2 "Create dir for challenge results"
    if [ -d "$RESULTS_DIR" ]; then
        mv "$RESULTS_DIR" "$RESULTS_DIR.$( date +%Y%m%d%H%M%S )"
    fi
    mkdir "$RESULTS_DIR"
}

function part1() {
    h1 "Part 1"

    h2 "Generate results for part 1a"
    hive --database $DB_NAME -f scripts/part1a.hql > $RESULTS_DIR/part1a.csv
    
    h2 "Generate results for part 1b"
    hive --database $DB_NAME -f scripts/part1b.hql > $RESULTS_DIR/part1b.csv
    
    h2 "Generate results for part 1c"
    hive --database $DB_NAME -f scripts/part1c.hql > $RESULTS_DIR/part1c.csv
    
    h2 "Generate results for part 1d"
    hive --database $DB_NAME -f scripts/part1d.hql > $RESULTS_DIR/part1d.csv
    
}

function part2() {
    h1 "Part 2"

    h2 "Generate results for part 2a"
    hive --database $DB_NAME -f scripts/part2a.hql > $RESULTS_DIR/part2a.csv
    
    h2 "Generate results for part 2b"
    hive --database $DB_NAME -f scripts/part2b.hql > $RESULTS_DIR/part2b.csv

}

function part3() {
    h1 "Part 3"

    h2 "Prepare data and extract features lor learning"
    hive -v -f scripts/part3_extractfeatures.hql \
        --database $DB_NAME \
        -hivevar inputdir=$INPUT_DIR \
        -hivevar outputdir=$OUTPUT_DIR

    h2 "Run clustering to identify negative labels"
    local clustering_log=/tmp/clustering.log.$$
    hdfs dfs -rm -R $OUTPUT_DIR/workdata $OUTPUT_DIR/mahoutdata $OUTPUT_DIR/clusteredclaims
    time hadoop jar $JAR_FILE \
        com.asdaraujo.KMeansClaims \
        2 \
        10 \
        $OUTPUT_DIR/patientclustering \
        $OUTPUT_DIR/workdata \
        $OUTPUT_DIR/mahoutdata \
        $OUTPUT_DIR/clusteredclaims \
        | tee $clustering_log
        
    h2 "Find negative labels cluster from the clustering output"
    POSITIVE_CLUSTER=$( grep "^Cluster.*Label 1" $clustering_log | sort -k5 -n | tail -1 | awk '{gsub(",", "", $2); print $2}' )
    NEGATIVE_CLUSTER=$(( 1 - POSITIVE_CLUSTER ))
    grep "^Cluster.*Label 1" $clustering_log
    echo "Cluster containing the negative labels is cluster #$NEGATIVE_CLUSTER"
    rm -f $clustering_log

    h2 "Prepare training and test inputs for the classifier"
    hive -v -f scripts/part3_prepareinput.hql \
        --database $DB_NAME \
        -hivevar outputdir=$OUTPUT_DIR \
        -hivevar negativeCluster=$NEGATIVE_CLUSTER

    h2 "Run classifier"
    hdfs dfs -rm -R classifiedclaims
    time hadoop jar $JAR_FILE \
        com.asdaraujo.ClaimClassifier \
        $OUTPUT_DIR/patienttraining \
        $OUTPUT_DIR/patienttest \
        $OUTPUT_DIR/classifiedclaims/data

    h2 "Isolate the top 10000 suspicious claims"
    hive -v -f scripts/part3_suspicious_claims.hql \
        --database $DB_NAME \
        -hivevar outputdir=$OUTPUT_DIR

    h2 "Write results file"
    hive -v -f scripts/part3_suspicious_claims.hql \
        --database $DB_NAME \
        -hivevar outputdir=$OUTPUT_DIR > $RESULTS_DIR/part3.csv

}

function main() {
    data_preparation
    part1
    part2
    part3
}

     __main__
main
