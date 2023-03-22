-- REVIEW Datamart
INSERT INTO datamart.review_by_location
select 
	fr.fr_location_id as location_id,
	db.name as venue_name,
	db.is_open as is_venue_open,
	db.categories,
	dl.city,
	dl.state,
	date(fr.date_review) date_review,
	dd.month_name,
	dd.day_name,
	fr.stars as stars_review,
	rt.min as min_temperature,
	rt.max as max_temperature,
	rp.percipitation as precipitation
from 
	dwh.fact_review fr 
left join dwh.dim_business db 
	on db.db_business_id = fr.fr_business_id 
left join dwh.dim_location dl 
	on dl.dl_location_id  = fr.fr_location_id 
left join dwh.dim_datetime dd 
	on dd.dd_datetime_id = fr.date_review
left join dwh.ref_percipitation rp 
	on rp.date = date(fr.date_review) 
left join dwh.ref_temperature rt 
	on rt.date = date(fr.date_review) 
	
