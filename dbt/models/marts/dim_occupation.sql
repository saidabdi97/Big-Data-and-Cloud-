-- Dimension: Yrkesområden
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY occupation_field) as occupation_key,
    occupation_field
FROM {{ ref('stg_job_ads') }}
WHERE occupation_field IS NOT NULL
