dfs -mkdir ${schemadir};
dfs -copyFromLocal src/main/avro/Patient.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/PatientClaim.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/InpatientData.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/OutpatientData.avsc ${schemadir}/;

CREATE EXTERNAL TABLE ${dbname}.patient
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/patient'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/Patient.avsc');

CREATE EXTERNAL TABLE ${dbname}.patientClaim
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/patientclaim'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/PatientClaim.avsc');

CREATE EXTERNAL TABLE ${dbname}.inpatientData
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/inpatient'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/InpatientData.avsc');

CREATE EXTERNAL TABLE ${dbname}.outpatientData
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION '${outputdir}/outpatient'
  TBLPROPERTIES (
    'avro.schema.url'='${schemadir}/OutpatientData.avsc');

