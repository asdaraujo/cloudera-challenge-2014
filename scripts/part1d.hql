select provider_id
from (
  select
    provider_id,
    count(*) cnt
  from (
    select
      procedure_id,
      bigint(provider_id) provider_id,
      row_number() over (partition by procedure_id order by double(avg_charged_amount-avg_total_payment) desc) rn
    from in_out_patient
  ) m
  where rn = 1
  group by provider_id
  order by cnt desc
) n
limit 3
;

