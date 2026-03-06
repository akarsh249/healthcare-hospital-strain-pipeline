-- Top states by hospital strain
SELECT
state,
AVG(strain_score) AS avg_strain
FROM gold.hospital_strain_alerts
GROUP BY state
ORDER BY avg_strain DESC;


-- Strain distribution
SELECT
strain_score
FROM gold.hospital_strain_alerts
WHERE strain_score IS NOT NULL;


-- Recent pipeline runs
SELECT *
FROM bronze.ingestion_runs
ORDER BY started_at DESC
LIMIT 20;