-- Dimension: Kommuner
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY municipality) as location_key,
    municipality
FROM {{ ref('stg_job_ads') }}
WHERE municipality IS NOT NULL
