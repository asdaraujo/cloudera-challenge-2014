select 
  regexp_replace(referral_region, '^(..) - (.*)$', '$2,$1')
from (
  select
    b.referral_region, 
    (avg_charged_amount-mean_avg_charged_amount)/sd_avg_charged_amount as z
  from
    (select 
       procedure_id, 
       avg(avg_charged_amount) as mean_avg_charged_amount,
       stddev(avg_charged_amount) as sd_avg_charged_amount
     from araujo.all_procedures
     group by procedure_id) a
    join
    (select
       procedure_id, referral_region,
       sum(total_charges*avg_charged_amount)/sum(total_charges) as avg_charged_amount
     from araujo.all_procedures
     group by procedure_id, referral_region) b
      on (a.procedure_id = b.procedure_id)
  order by z desc
  limit 3
) x
;
