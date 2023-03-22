-- TIP Datamart
INSERT INTO datamart.tip_by_location
select 
	ft.ft_location_id as location_id,
	db.name as venue_name,
	db.is_open as is_venue_open,
	db.categories,
	dl.city,
	dl.state,
	date(ft.date_tip),
	dd.month_name,
	dd.day_name,
	ft.compliment_count,
	rt.min as min_temperature,
	rt.max as max_temperature,
	rp.percipitation as precipitation
from 
	dwh.fact_tip ft
left join dwh.dim_business db 
	on db.db_business_id = ft.ft_business_id 
left join dwh.dim_location dl 
	on dl.dl_location_id  = ft.ft_location_id 
left join dwh.dim_datetime dd 
	on dd.dd_datetime_id = ft.date_tip 
left join dwh.ref_percipitation rp 
	on rp.date = date(ft.date_tip) 
left join dwh.ref_temperature rt 
	on rt.date = date(ft.date_tip) 
	
