{\rtf1\ansi\ansicpg1252\cocoartf1404\cocoasubrtf470
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
\margl1440\margr1440\vieww11940\viewh7800\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 alter table crash_data add columns (latitude double, longitude double);\
insert overwrite table crash_data select latlong, crashes, split(latlong, ',')[0] as latitude, split(latlong, ',')[1] as longitude from crash_data;\
\
alter table complaint_data add columns (latitude double, longitude double);\
insert overwrite table complaint_data select latlong, complaints, split(latlong, '"\\\\(|, |\\\\)"')[1] as latitude, split(latlong, '"\\\\(|, |\\\\)"')[2] as longitude from complaint_data;}