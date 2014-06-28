select regexp_replace(referral_region, '(..) - (.*)', '$2,$1')
from (
  select
    referral_region,
    count(*) cnt
  from (
    select
      procedure_id,
      referral_region,
      row_number() over (partition by procedure_id order by avg_per_region desc) rn
    from (
      select
        procedure_id,
        referral_region,
        sum(total_charges * avg_charged_amount)/sum(total_charges) avg_per_region
      from in_out_patient
      group by procedure_id, referral_region
    ) m
  ) n
  where rn = 1
  group by referral_region
  order by cnt desc
) o
limit 3
;

