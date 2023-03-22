INSERT INTO dwh.ref_temperature
SELECT 
	to_date(date::text, 'YYYYMMDD'),
	max as min_temp,
	min as max_temp,
	normal_max as normal_min_temp,
	normal_min as normal_max_temp
FROM raw.temperature_stg

