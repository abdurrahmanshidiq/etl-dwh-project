insert into dwh.fact_review 
select 
	cast(rs.review_id as text) as fr_review_id,
	cast(rs.user_id as text) as fr_user_id,
	cast(rs.business_id as text)fr_business_id,
	dl.dl_location_id as fr_location_id,
	cast(rs.stars as double precision),
	cast(rs.usefull as integer),
	cast(rs.funny as integer),
	cast(rs.cool as integer),
	cast(rs.text as text),
	cast(rs.date as timestamp)
from
	raw.review_stg rs
join raw.business_stg bs 
	on rs.business_id  = bs.business_id 
join dwh.dim_location dl 
	on bs.city = dl.city and bs.state = dl.state  and bs.postal_code = dl.postal_code 