select regexp_replace(referral_region, '(..) - (.*)', '$2,$1')
from (
  select referral_region, count(*) cnt
  from (
    select code, referral_region, row_number() over (partition by code order by avg_per_region desc) rn
    from (
      select drg code, referral_region, sum(total_discharges * average_covered_charges)/sum(total_discharges) avg_per_region
      from inpatientdata
      group by drg, referral_region
      union all
      select apc code, referral_region, sum(outpatient_services * average_est_submitted_charges)/sum(outpatient_services) avg_per_region
      from outpatientdata
      group by apc, referral_region
    ) m
  ) n
  where rn = 1
  group by referral_region
  order by cnt desc
) o
limit 3
;

