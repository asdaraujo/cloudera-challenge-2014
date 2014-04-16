set hive.cli.errors.ignore=true;
dfs -rm -r schema/;

DROP TABLE araujo.patient;
DROP TABLE araujo.patientClaim;
DROP TABLE araujo.inpatientData;
DROP TABLE araujo.outpatientData;
