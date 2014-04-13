dfs -mkdir schema/;
dfs -copyFromLocal src/main/avro/Patient.avsc schema/;
dfs -copyFromLocal src/main/avro/PatientClaim.avsc schema/;

CREATE EXTERNAL TABLE araujo.patient
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION 'hdfs:///user/araujo/patient'
  TBLPROPERTIES (
    'avro.schema.url'='hdfs:///user/araujo/schema/Patient.avsc');

CREATE EXTERNAL TABLE araujo.patientClaim
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  LOCATION 'hdfs:///user/araujo/patientclaim'
  TBLPROPERTIES (
    'avro.schema.url'='hdfs:///user/araujo/schema/PatientClaim.avsc');

