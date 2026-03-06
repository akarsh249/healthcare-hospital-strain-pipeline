CREATE OR REPLACE VIEW gold.state_strain_summary AS
SELECT
    state,
    COUNT(*) AS hospitals_reporting,
    AVG(strain_score) AS avg_strain_score
FROM gold.hospital_strain_alerts
WHERE strain_score IS NOT NULL
GROUP BY state
ORDER BY avg_strain_score DESC;