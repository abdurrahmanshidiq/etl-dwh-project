INSERT INTO dwh.ref_temperature
SELECT 
	to_date(date::text, 'YYYYMMDD'),
	min,
	max,
	normal_min,
	normal_max
FROM raw.temperature_stg

