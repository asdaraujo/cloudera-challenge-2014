#!/bin/bash

readonly DB_NAME=${1:-$USER}
echo "Hive db name: $DB_NAME"
readonly OUTPUT_DIR=${2:-hdfs://$( hdfs getconf -nnRpcAddresses )/user/$USER/ccoutput}
echo "Output dir: $OUTPUT_DIR"
readonly SCHEMA_DIR=${3:-hdfs://$( hdfs getconf -nnRpcAddresses )/user/$USER/schema}
echo "Schema dir: $SCHEMA_DIR"
readonly RESULTS_DIR=${4:-results}
echo "Results dir: $RESULTS_DIR"

#hdfs dfs -stat $OUTPUT_DIR > /dev/null 2>&1
#if [ $? == 0 ]; then
#  hdfs dfs -mv $OUTPUT_DIR $OUTPUT_DIR.$( date +%Y%m%d%H%M%S )
#fi
#hdfs dfs -mkdir -p $OUTPUT_DIR
#
#hadoop jar target/araujo-ccds2-1.0-SNAPSHOT-job.jar \
#  ccinput/patientclaim \
#  ccinput/patient \
#  ccinput/inpatient \
#  ccinput/outpatient \
#  $OUTPUT_DIR/patientclaim \
#  $OUTPUT_DIR/patient \
#  $OUTPUT_DIR/inpatient \
#  $OUTPUT_DIR/outpatient
#
#hive -v -f scripts/cleanupHive.hql
#hive -v -f scripts/setupHive.hql -hivevar dbname=$DB_NAME -hivevar outputdir=$OUTPUT_DIR -hivevar schemadir=$SCHEMA_DIR
#
## check for results dir
#if [ -d "$RESULTS_DIR" ]; then
#  mv "$RESULTS_DIR" "$RESULTS_DIR.$( date +%Y%m%d%H%M%S )"
#fi
#mkdir "$RESULTS_DIR"
#
## part 1a
#hive --database $DB_NAME -f scripts/part1a.hql > $RESULTS_DIR/part1a.csv
#
## part 1b
#hive --database $DB_NAME -f scripts/part1b.hql > $RESULTS_DIR/part1b.csv
#
## part 1c
#hive --database $DB_NAME -f scripts/part1c.hql > $RESULTS_DIR/part1c.csv
#
## part 1d
#hive --database $DB_NAME -f scripts/part1d.hql > $RESULTS_DIR/part1d.csv
#
# part 2a
hive --database $DB_NAME -f scripts/part2a.hql > $RESULTS_DIR/part2a.csv

# part 2b
hive --database $DB_NAME -f scripts/part2b.hql > $RESULTS_DIR/part2b.csv
#
