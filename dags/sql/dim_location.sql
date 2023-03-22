INSERT INTO dwh.dim_location
SELECT 
    DISTINCT 
        md5(postal_code || city || state) as dl_location_id,
        city,
        state,
        postal_code
FROM 
    raw.business_stg
WHERE
    postal_code IS NOT NULL