select regexp_replace(code, ' .*', '')
from (
  select code, rv
  from (
    select drg code, power(stddev(average_covered_charges)/avg(average_covered_charges), 2) rv from inpatientdata group by drg
    UNION ALL
    select apc code, power(stddev(average_est_submitted_charges)/avg(average_est_submitted_charges), 2) rv from outpatientdata group by apc
  ) m
  order by rv desc
  limit 3
) n;

