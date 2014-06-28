-- Hive table to read the classified data
drop table if exists classifiedclaims;
create external table classifiedclaims (
  patient_id string,
  review int,
  score double
)
stored as sequencefile
location '${outputdir}/classifiedclaims'
;

-- create table containing the top 10000 suspicious claims
drop table if exists claimsforreview;
create table claimsforreview
as
select *
from (
  select *
  from default.classifiedclaims
  where review = 1
  order by score desc
) c
limit 10000
;

select patient_id
from claimsforreview;
