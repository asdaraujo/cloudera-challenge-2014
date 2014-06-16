select 
  provider_id
from (
  select
    b.provider_id,
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
       procedure_id, provider_id,
       sum(total_charges*avg_charged_amount)/sum(total_charges) as avg_charged_amount
     from araujo.all_procedures
     group by procedure_id, provider_id) b
      on (a.procedure_id = b.procedure_id)
  order by z desc
  limit 3
) x
;
