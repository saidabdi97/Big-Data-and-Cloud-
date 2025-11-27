-- Dimension: Arbetsgivare
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY employer) as employer_key,
    employer
FROM {{ ref('stg_job_ads') }}
WHERE employer IS NOT NULL
