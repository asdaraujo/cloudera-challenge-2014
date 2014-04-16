dfs -mkdir schema/;
dfs -copyFromLocal src/main/avro/Patient.avsc schema/;
dfs -copyFromLocal src/main/avro/PatientClaim.avsc schema/;
dfs -copyFromLocal src/main/avro/InpatientData.avsc schema/;
dfs -copyFromLocal src/main/avro/OutpatientData.avsc schema/;

CREATE EXTERNAL TABLE araujo.patient
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION 'hdfs:///user/araujo/ccoutput/patient'
  TBLPROPERTIES (
    'avro.schema.url'='hdfs:///user/araujo/schema/Patient.avsc');

CREATE EXTERNAL TABLE araujo.patientClaim
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION 'hdfs:///user/araujo/ccoutput/patientclaim'
  TBLPROPERTIES (
    'avro.schema.url'='hdfs:///user/araujo/schema/PatientClaim.avsc');

CREATE EXTERNAL TABLE araujo.inpatientData
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION 'hdfs:///user/araujo/ccoutput/inpatient'
  TBLPROPERTIES (
    'avro.schema.url'='hdfs:///user/araujo/schema/InpatientData.avsc');

CREATE EXTERNAL TABLE araujo.outpatientData
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION 'hdfs:///user/araujo/ccoutput/outpatient'
  TBLPROPERTIES (
    'avro.schema.url'='hdfs:///user/araujo/schema/OutpatientData.avsc');

