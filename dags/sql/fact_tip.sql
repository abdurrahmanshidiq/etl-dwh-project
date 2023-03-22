INSERT INTO dwh.fact_tip
select 
	DISTINCT md5(ts.user_id || ts.business_id || ts.date || ts.text) as ft_tip_id,
	cast(ts.user_id as text) as ft_user_id,
	cast(ts.business_id as text) as ft_business_id,
	dl.dl_location_id as ft_location_id,
	cast(text as text),
	cast(date as timestamp) as date_tip,
	cast(compliment_count as integer)
from
	raw.tip_stg ts
join raw.business_stg bs 
	on ts.business_id  = bs.business_id 
join dwh.dim_location dl 
	on bs.city = dl.city and bs.state = dl.state  and bs.postal_code = dl.postal_code 