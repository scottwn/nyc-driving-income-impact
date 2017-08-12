create external table crash_data (zip string, crashes int)
row format delimited fields terminated by '\011'
location '/user/swn226/project/crash_data/';
create external table complaint_data (zip string, complaints int)
row format delimited fields terminated by '\011'
location '/user/rs4070/countdata2/';
create external table commuters (PUMA string, commuters int)
row format delimited fields terminated by '\011'
location '/user/jc7228/project/output';
create external table zip_to_puma (zip string, puma string, name string, proportion float)
row format delimited fields terminated by '\073'
location '/user/swn226/project/zip_to_puma/';
create table crash_data_2 as select crash_data.crashes, zip_to_puma.* from crash_data join zip_to_puma on (crash_data.zip = zip_to_puma.zip);
create table crash_data_3 as select puma, crashes * proportion as crashes from crash_data_2 sort by puma asc;
create table crash_data_4 as select puma, sum(crashes) as crashes from crash_data_3 group by puma;
create table complaint_data_2 as select complaint_data.complaints, zip_to_puma.* from complaint_data join zip_to_puma on (complaint_data.zip = zip_to_puma.zip);
create table complaint_data_3 as select puma, complaints * proportion as complaints from complaint_data_2 sort by puma asc;
create table complaint_data_4 as select puma, sum(complaints) as complaints from complaint_data_3 group by puma;
create table commuters_crashes as select commuters.*, crash_data_4.crashes from commuters join crash_data_4 on (commuters.puma = crash_data_4.puma);
create table commuters_crashes_complaints as select commuters_crashes.*, complaint_data_4.complaints from commuters_crashes join complaint_data_4 on (commuters_crashes.puma = complaint_data_4.puma);
create table analytic_table as select commuters_crashes_complaints.*, zip_to_puma.name from commuters_crashes_complaints join zip_to_puma on (commuters_crashes_complaints.puma = zip_to_puma.puma);
insert overwrite table analytic_table select distinct * from analytic_table sort by puma asc;
