select regexp_replace(procedure_id, ' .*', '')
from (
  select
    procedure_id,
    rv
  from (
    select
      procedure_id,
      power(stddev(avg_charged_amount)/avg(avg_charged_amount), 2) rv
    from in_out_patient
    group by procedure_id
  ) m
  order by rv desc
  limit 3
) n;

