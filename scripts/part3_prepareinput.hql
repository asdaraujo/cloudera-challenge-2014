-- create partitioned table to query the clustered data
drop table if exists clusteredclaims;
create external table clusteredclaims (
  patient_id string
)
partitioned by (cluster int, label int)
stored as sequencefile
location '${outputdir}/clusteredclaims'
;
alter table clusteredclaims add partition (cluster = 0, label = 0);
alter table clusteredclaims add partition (cluster = 0, label = 1);
alter table clusteredclaims add partition (cluster = 1, label = 0);
alter table clusteredclaims add partition (cluster = 1, label = 1);


-- based on the output above, select samples for training
drop table if exists patienttraining;
create table patienttraining like patientfeatures location '${outputdir}/patienttraining';
insert overwrite table patienttraining
select * from (
  select * from patientfeatures where review = 1
  union all
  select f.*
  from patientfeatures f
    join (select patient_id from clusteredclaims where cluster = ${negativeCluster} and label = 0) c on f.id = c.patient_id
  where f.review = 0 order by rand() limit 50000
) x
;

-- select samples for TESTing
drop table if exists patienttest;
create table patienttest like patientfeatures location '${outputdir}/patienttest';
insert overwrite table patienttest
select f.*
from (select * from patientfeatures where review = 0) f
  left outer join (select patient_id from clusteredclaims where cluster = ${negativeCluster} and label = 0) c
    on f.id = c.patient_id
where c.patient_id is null
;

