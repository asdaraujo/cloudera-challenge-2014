#!/bin/bash

readonly OUTPUT_DIR=${1:-ccoutput}
echo "Output dir: $OUTPUT_DIR"

hdfs dfs -stat $OUTPUT_DIR > /dev/null 2>&1
if [ $? == 0 ]; then
  echo "ERROR: Output dir already exists."
  exit 1
fi
hdfs dfs -mkdir -p $OUTPUT_DIR

hadoop jar target/araujo-ccds2-1.0-SNAPSHOT-job.jar \
  ccinput/patientclaim \
  ccinput/patient \
  ccinput/inpatient_unzipped \
  ccinput/outpatient \
  $OUTPUT_DIR/patientclaim \
  $OUTPUT_DIR/patient \
  $OUTPUT_DIR/inpatient \
  $OUTPUT_DIR/outpatient

