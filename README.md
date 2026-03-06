# US Hospital Strain Monitoring Pipeline

This project builds an end-to-end healthcare data pipeline that ingests hospital capacity data from the CDC, processes hospital utilization metrics, and visualizes hospital system strain across U.S. states using an interactive Tableau dashboard.

---

## Architecture

CDC API  
↓  
Airflow DAG (Pipeline Orchestration)  
↓  
MinIO (Bronze Data Lake Storage)  
↓  
PostgreSQL (Analytics Warehouse)  
↓  
Tableau Dashboard (Visualization)

---

## Technologies Used

- Python
- Apache Airflow
- Docker
- MinIO (Object Storage)
- PostgreSQL
- SQL
- Tableau

---

## Pipeline Workflow

1. Hospital operations data is ingested from the CDC API.
2. Raw JSON data is stored in MinIO as the bronze layer.
3. Airflow processes hospital capacity metrics and calculates strain scores.
4. Processed data is stored in PostgreSQL analytics tables.
5. Tableau connects to PostgreSQL to visualize hospital strain insights.

---

## Key Features

- Automated data ingestion pipeline using Airflow
- Hospital strain scoring algorithm
- Data quality monitoring
- PostgreSQL analytics warehouse
- Interactive Tableau dashboard

---

## Dashboard Overview
![Dashboard](screenshots/dashboard_overview.png)

## Hospital Strain Map
![Strain Map](screenshots/strain%20map.png)

## Top States by Strain
![Top States](screenshots/top_states.png)

## Strain Distribution
![Distribution](screenshots/distribution_chart.png)
---

## Example SQL Query

```sql
SELECT state, AVG(strain_score)
FROM gold.hospital_strain_alerts
GROUP BY state
ORDER BY AVG(strain_score) DESC;
```

---

## Future Improvements

- Add streaming ingestion with Kafka
- Implement dbt transformation layer
- Add real-time alert notifications
- Deploy pipeline on cloud infrastructure

---

## Author

Akarsh Thota  
Master's in Management Information Systems  

Northern Illinois University
