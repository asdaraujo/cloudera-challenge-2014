set hive.cli.errors.ignore=true;
dfs -rm -r ${schemadir};
set hive.cli.errors.ignore=false;
dfs -mkdir ${schemadir};
dfs -copyFromLocal src/main/avro/Patient.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/PatientClaim.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/InpatientData.avsc ${schemadir}/;
dfs -copyFromLocal src/main/avro/OutpatientData.avsc ${schemadir}/;

drop table if exists patient;
create external table patient
  row format serde
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  stored as inputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  outputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  location '${outputdir}/patient'
  tblproperties (
    'avro.schema.url'='${schemadir}/Patient.avsc');

drop table if exists patientclaim;
create external table patientclaim
  row format serde
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  stored as inputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  outputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  location '${outputdir}/patientclaim'
  tblproperties (
    'avro.schema.url'='${schemadir}/PatientClaim.avsc');

drop table if exists inpatientdata;
create external table inpatientdata
  row format serde
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  stored as inputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  outputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  location '${outputdir}/inpatient'
  tblproperties (
    'avro.schema.url'='${schemadir}/InpatientData.avsc');

drop table if exists outpatientdata;
create external table outpatientdata
  row format serde
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  stored as inputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  outputformat
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  location '${outputdir}/outpatient'
  tblproperties (
    'avro.schema.url'='${schemadir}/OutpatientData.avsc');

-- all summary data (inpatient + outpatient)
drop table if exists in_out_patient;
create table in_out_patient
stored as sequencefile
as
select *
from (
  select
    'I' type, drg procedure_id, provider_id, provider_city, provider_state, provider_zipcode,
    referral_region, total_discharges total_charges, average_covered_charges avg_charged_amount,
    average_total_payments avg_total_payment
  from inpatientdata
  union all
  select
    'O' type, apc procedure_id, provider_id, provider_city, provider_state, provider_zipcode,
    referral_region, outpatient_services total_charges, average_est_submitted_charges avg_charged_amount,
    average_total_payments avg_total_payment
  from outpatientdata
) x
;

