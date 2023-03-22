INSERT INTO dwh.dim_datetime
select 
	distinct 
	dt.datetime as dd_datetime_id,
	cast(dt.datetime as date) as full_date,
	extract(year from dt.datetime) as year,
	extract(month from dt.datetime) as month,
	extract(day from dt.datetime) as day,
	to_char(date(dt.datetime), 'Month') month_name,
	to_char(date(dt.datetime), 'Day') day_name
from
	(
		select cast(yelping_since as timestamp) as datetime
		from raw.user_stg
		group by 1
		
		union 
		
		select cast(date as timestamp)
		from raw.review_stg
		group by 1
		
		union 
		
		select cast(date as timestamp)
		from raw.tip_stg
		group by 1
	) as dt
where 
	dt.datetime is not null
	
	
	
	
	
	