INSERT INTO dwh.dim_business
SELECT 
    cast(business_id as text), 
    cast(name as text), 
    cast(address as text),
    cast(latitide as decimal),
    cast(longitude as decimal),
    cast(is_open as integer),
    cast(stars as numeric),
    cast(categories as text)
FROM 
    raw.business_stg
WHERE
    cast(business_id as text) is not null;