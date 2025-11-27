-- Fact table: Jobbannonser
SELECT
    s.job_id,
    o.occupation_key,
    l.location_key,
    e.employer_key,
    s.title,
    s.transformed_at
FROM {{ ref('stg_job_ads') }} s
LEFT JOIN {{ ref('dim_occupation') }} o ON s.occupation_field = o.occupation_field
LEFT JOIN {{ ref('dim_location') }} l ON s.municipality = l.municipality
LEFT JOIN {{ ref('dim_employer') }} e ON s.employer = e.employer
