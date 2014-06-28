set hive.cli.errors.ignore=true;
dfs -rm -r ${schemadir};
set hive.cli.errors.ignore=false;
dfs -mkdir ${schemadir};
dfs -copyFromLocal src/main/avro/Patient.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/PatientClaim.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/InpatientData.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/OutpatientData.avsc ${schemadir}/;

DROP TABLE IF EXISTS patient;
CREATE EXTERNAL TABLE patient
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/patient'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/Patient.avsc');

DROP TABLE IF EXISTS patientClaim;
CREATE EXTERNAL TABLE patientClaim
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/patientclaim'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/PatientClaim.avsc');

DROP TABLE IF EXISTS inpatientData;
CREATE EXTERNAL TABLE inpatientData
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/inpatient'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/InpatientData.avsc');

DROP TABLE IF EXISTS outpatientData;
CREATE EXTERNAL TABLE outpatientData
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/outpatient'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/OutpatientData.avsc');

