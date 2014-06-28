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


-- unique procedures
drop table if exists procedure;
create table procedure
stored as sequencefile
as
select
  distinct regexp_replace(procedure_id, '^ *([0-9]*).*', '$1') as id,
  type,
  procedure_id as full_name
from in_out_patient
;


-- suspicious records identified for review
drop table if exists review;
create table review (
  patient_id string
)
row format delimited
fields terminated by ','
stored as textfile
;

set hive.cli.errors.ignore=true;
dfs -rm -r ${inputdir}/review/REVIEW.TXT.copy;
set hive.cli.errors.ignore=false;
dfs -cp ${inputdir}/review/REVIEW.TXT ${inputdir}/review/REVIEW.TXT.copy;
load data inpath '${inputdir}/review/REVIEW.TXT.copy' overwrite into table review;


-- patient features for clustering and training
drop table if exists patientfeatures;
create external table patientfeatures (
  id string,
  review string,
  age string,
  gender string,
  income string,
  type_I string,
  type_O string,
  claims string
)
stored as rcfile
;

insert overwrite table patientfeatures
select
  id, 
  review,
  age,
  gender,
  income,
  sum(if(type = 'I', num_claims, 0)) type_I,
  sum(if(type = 'O', num_claims, 0)) type_O,
  concat_ws(',', collect_set(concat(claim_id, ',', num_claims)))
from (
  select
    p.id, 
    if(r.patient_id is null, 0, 1) as review,
    p.age,
    p.gender,
    p.income,
    q.type,
    c.claim_id,
    count(1) num_claims
  from patient p
    join patientclaim c on c.patient_id = p.id
    join procedure q on q.id = c.claim_id
    left outer join review r on r.patient_id = p.id
  group by
    p.id, 
    if(r.patient_id is null, 0, 1),
    p.age,
    p.gender,
    p.income,
    q.type,
    c.claim_id
) patient_claims
group by 
  id, 
  review,
  age,
  gender,
  income
;


-- extract from the feature table random samples to be used for clustering
drop table if exists patientclustering;
create table patientclustering like patientfeatures location '${outputdir}/patientclustering';
insert overwrite table patientclustering
select * from (
  select * from patientfeatures where review = 1
  union all
  select * from patientfeatures where review = 0 order by rand() limit 150000
) x
;

