-- Staging: Kombinera alla jobbområden till en tabell
WITH all_jobs AS (
    SELECT * FROM {{ source('job_ads_dataset', 'job_ads_data') }}
    UNION ALL
    SELECT * FROM {{ source('job_ads_dataset', 'job_ads_hr') }}
    UNION ALL
    SELECT * FROM {{ source('job_ads_dataset', 'job_ads_ekonomi') }}
)

SELECT
    _dlt_id as job_id,
    title,
    employer,
    municipality,
    occupation as occupation_field,
    _dlt_load_id,
    CURRENT_TIMESTAMP as transformed_at
FROM all_jobs
WHERE title IS NOT NULL
