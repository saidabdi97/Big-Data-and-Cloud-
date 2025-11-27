CREATE OR REPLACE TABLE job_ads_dataset.job_ads_summary AS
SELECT
    occupation AS yrkesomrade,
    municipality AS kommun,
    COUNT(*) AS antal_annonser
FROM job_ads_dataset.job_ads_data
GROUP BY occupation, municipality
ORDER BY antal_annonser DESC;
