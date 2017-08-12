ADD JAR hdfs://babar.es.its.nyu.edu:8020/user/jc7228/esri-geometry-api.jar;
ADD JAR hdfs://babar.es.its.nyu.edu:8020/user/jc7228/spatial-sdk-hadoop.jar;
create temporary function ST_GeomFromGeoJson as 'com.esri.hadoop.hive.ST_GeomFromGeoJson';
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
select nyc_shapes.name, count(*) cnt from nyc_shapes 
join crash_data 
where ST_Contains(ST_GeomFromGeoJson(nyc_shapes.shape), ST_Point(crash_data.longitude, crash_data.latitude)) 
group by nyc_shapes.name 
order by cnt desc;