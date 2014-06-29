select
  regexp_replace(referral_region, '^(..) - (.*)$', '$2,$1')
from (
  select
    p.referral_region,
    p.type,
    stddev((p.avg_charged_amount-s.mean_avg_charged_amount)/s.sd_avg_charged_amount) as sd_z
  from
    (select
       procedure_id,
       avg(avg_charged_amount) as mean_avg_charged_amount,
       stddev(avg_charged_amount) as sd_avg_charged_amount
     from in_out_patient
     group by procedure_id) s
    join in_out_patient p
      on (p.procedure_id = s.procedure_id)
  group by p.referral_region, p.type
  order by sd_z desc
  limit 3
) x
;

