set hive.cli.errors.ignore=true;
dfs -rm -r schema/;

DROP TABLE araujo.patient;
DROP TABLE araujo.patientClaim;
