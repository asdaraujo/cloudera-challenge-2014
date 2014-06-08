select provider_id
from (
  select provider_id, count(*) cnt
  from (
    select drg code, bigint(provider_id) provider_id, row_number() over (partition by drg order by double(average_covered_charges) desc) rn
    from inpatientdata
    union all
    select apc code, bigint(provider_id) provider_id, row_number() over (partition by apc order by double(average_est_submitted_charges) desc) rn
    from outpatientdata
  ) m
  where rn = 1
  group by provider_id
  order by cnt desc
) n
limit 3
;

