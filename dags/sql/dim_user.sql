INSERT INTO dwh.dim_user
SELECT 
	cast(user_id as text),
	cast(name as text),
	cast(yelping_since as timestamp)
FROM 
    raw.user_stg