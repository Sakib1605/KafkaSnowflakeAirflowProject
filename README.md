# 🚀 End-to-End Real-Time Streaming Pipeline  
### *Kafka • MinIO • Airflow • Snowflake • dbt • Power BI*

This project showcases a **fully automated, production-style real-time data pipeline** built using the Modern Data Stack. Live ride events flow through a complete ecosystem—streaming → storage → orchestration → warehouse → modeling → dashboards—following the Medallion Architecture (Bronze → Silver → Gold).

---

## 🔧 Tools & Technologies Used
- **Kafka** – real-time streaming backbone  
- **Python + Faker** – synthetic event generator  
- **MinIO** – S3-compatible object storage for Bronze raw data  
- **Airflow** – workflow scheduling & ingestion automation  
- **Snowflake** – cloud data warehouse with Medallion layers  
- **dbt** – transformations, modeling, testing, lineage  
- **Docker** – containerized deployment  
- **GitHub Actions** – CI/CD for dbt build & validation  
- **Power BI** – visualization layer for business insights  

---

## 🔁 End-to-End Pipeline (Short Summary)

- Generate continuous ride events (trips, surge pricing, driver updates)  
- Stream events into **Kafka**  
- Land raw JSON events into **MinIO Bronze layer**  
- Use **Airflow** to ingest data into **Snowflake Bronze**  
- Clean and standardize data into **Silver**  
- Transform into **Gold fact & dimension tables** using **dbt**  
- Build dashboards by connecting **Power BI** to Snowflake Gold  
- Automate deployments with **GitHub Actions CI/CD**  

---
# 🧱 High-Level Architecture
<img width="1472" height="704" alt="Pipeline" src="https://github.com/user-attachments/assets/48cd12f4-d61e-4eeb-8112-673c432d3bf4" />

---


# Workflow:
### Documentation: https://medium.com/@sakibul1605/how-i-designed-an-end-to-end-streaming-pipeline-with-kafka-minio-airflow-snowflake-dbt-and-18c489fb4f9d

# 🧠 1. Real-Time Event Generator (Python + Kafka)

A Python-based event simulator produces continuous ride-sharing events:
- Trip lifecycle (requested, assigned, started, completed)  
- Surge pricing changes  
- Driver location updates  

These events form the real-time operational heartbeat of the system.

---

# 📡 2. Kafka → MinIO Bronze Layer

A Kafka consumer collects streaming events and writes them into MinIO in a **time-partitioned** folder structure.

MinIO serves as the raw Bronze landing zone, storing NDJSON event batches.

---

# ⚙️ 3. Airflow Orchestration (MinIO → Snowflake Bronze)

Airflow manages ingestion through two key tasks:

### **Task 1 – extract_data**
- Reads raw NDJSON files from MinIO  
- Consolidates them into a batch  

### **Task 2 – load_raw_to_snowflake**
- Loads consolidated events into Snowflake Bronze  
- Ensures structured, traceable ingestion  

Airflow provides scheduling, retries, logging, and lineage.

---

# 🏛️ 4. Snowflake Medallion Architecture + dbt

All warehousing is organized into Medallion layers:

## 🥉 Bronze Layer  
- Raw, unprocessed events  
- Immutable audit log  
- Maintains event-level history  

## 🥈 Silver Layer  
- Cleaned and standardized event schema  
- Parsed timestamps, corrected types  
- Duplicate handling and validation  

## 🥇 Gold Layer  
**Star-schema analytics tables** built using dbt:

### **Dimensions**
- Users  
- Drivers  
- Regions  
- Date  

### **Fact Tables**
- Completed trips  
- Driver daily metrics  
- Surge pricing patterns  

Gold tables enable fast, consistent BI analysis.

---

# 🔧 5. CI/CD Pipeline (GitHub Actions)

Every push triggers automated:

- Python environment setup  
- dbt installation  
- Snowflake connection setup  
- dbt compile validation  
- dbt run transformation  
- Automated deployment confirmation  

This ensures clean, testable, and production-ready transformations.

---

# 📊 6. Power BI Dashboards

Power BI connects directly to Snowflake’s Gold layer to visualize:

- Trip and revenue trends  
- Region-wise demand  
- Driver performance metrics  
- Surge intensity patterns  

A star-schema model enables smooth slicers and flexible Q&A by business teams.

---

# 🧾 7. Project Highlights

- Real-time event streaming architecture  
- Automated data ingestion with Airflow  
- Medallion Architecture implemented in Snowflake  
- dbt transformations with data quality enforcement  
- Fully containerized system using Docker  
- CI/CD automation using GitHub Actions  
- Analytical dashboards powered by Power BI  

---

# 🎯 Conclusion

This project unifies the best elements of today’s **modern data ecosystem**:

This project brings together the best of today's modern data ecosystem - Kafka, MinIO, Airflow, Snowflake, dbt, and Power BI to demonstrate how raw operational events can be transformed into reliable, business-ready insights. Each tool plays a strategic role in the pipeline, and together they create a seamless flow of data from ingestion all the way to executive reporting.
Kafka ensures that every event is captured in real time, providing the kind of live operational visibility that fast-moving businesses depend on. MinIO acts as the raw data landing zone, giving the organization a scalable and cheap storage layer. Airflow removes manual effort entirely by orchestrating the continuous movement of data into Snowflake, guaranteeing that pipelines run consistently and fault-tolerantly.
Inside Snowflake, we implemented a clean Medallion Architecture - Bronze, Silver, Gold - supported by dbt transformations. This combination delivers governed, trusted data modeling with automated testing, documentation, and lineage tracking. It ensures that what leaders see in reports is accurate, validated, and aligned with business definitions.
Finally, Power BI brings all of this engineering work to life. Through a star-schema model and multiple curated report pages, stakeholders can track revenue, trips, driver performance, and surge behavior in a way that is intuitive, interactive, and always up to date.

Together, they create a **fully automated streaming analytics platform** similar to what real ride-sharing companies run in production.


