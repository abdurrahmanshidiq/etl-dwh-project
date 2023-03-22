INSERT INTO dwh.ref_percipitation
SELECT 
	to_date(date::text, 'YYYYMMDD'),
	cast(precipitation as float),
	cast(precipitation_normal as float)
FROM raw.percipitation_stg



