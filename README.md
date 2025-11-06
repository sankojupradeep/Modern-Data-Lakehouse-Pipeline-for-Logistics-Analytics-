# 🚚 Logistics Data Lakehouse Pipeline

## 📖 Project Overview
The **Logistics Data Lakehouse Pipeline** is an end-to-end data engineering project designed to automate the ingestion, transformation, and analytics of shipment tracking data from multiple logistics sources such as **AfterShip** and **Correios**.

The pipeline follows a **multi-layered Lakehouse architecture (Bronze → Silver → Gold)** and integrates with **Snowflake** for reporting and **Power BI** for visualization.

This project simulates a real-world data engineering workflow commonly used in logistics, e-commerce, and supply chain analytics to monitor **shipment performance, delivery accuracy, and courier efficiency**.


---

## 🏗️ Key Objectives
- Build a scalable data pipeline to handle high-volume logistics data (50K–90K daily records).  
- Implement a multi-layered data architecture using **Bronze**, **Silver**, and **Gold** layers for raw, cleaned, and analytical data.  
- Use **Apache Airflow** to orchestrate and schedule ETL workflows for daily automation.  
- Leverage **Storj (S3-compatible)** as a data lake for cost-efficient and secure cloud storage.  
- Integrate **Snowflake** as the data warehouse for analytics and **Power BI** dashboard connectivity.  
- Train a **Machine Learning model** on Gold Layer data to predict delivery times and shipment success probabilities.  

---

## 🧱 Pipeline Summary

### Bronze Layer – Raw Data Ingestion
- Extract raw shipment tracking data from AfterShip APIs or generate synthetic data using **Faker**.  
- Store the unprocessed data in **Storj cloud storage** (S3-compatible) in date-wise partitions.  

### Silver Layer – Data Cleaning & Transformation
- Use **PySpark** to clean, validate, and standardize the raw data.  
- Merge datasets, handle nulls, correct schemas, and enrich fields (e.g., delivery duration, courier normalization).  
- Store processed data back in **Storj** under the Silver layer.  

### Gold Layer – Analytical Model Creation
- Transform the Silver data into **Fact** and **Dimension** tables using a **Star Schema** design.  
- Compute business metrics like average delivery time, courier success rate, and shipment delays.  
- Store results in **Gold Layer** folders for daily analysis.  

---

## ❄️ Snowflake Integration & 📊 Power BI Reporting
- Load **Gold Layer tables** into Snowflake for warehousing and BI consumption.  
- Build **Power BI dashboards** for visualizing delivery performance, delays, and courier KPIs.  

---

## 🤖 Machine Learning & Prediction API
- Train an ML model using **Gold Layer data** to predict:
  - Delivery success probability  
  - Expected delivery date  
- Serve real-time predictions through a **FastAPI** service endpoint.  

---

## ⚙️ Why This Project Matters
This project demonstrates **real-world data engineering concepts end-to-end**:
- Data ingestion, transformation, and modeling across multiple layers.  
- Automation using **Apache Airflow DAGs**.  
- Scalable data processing with **PySpark**.  
- Integration with **Storj** and **Snowflake** for cloud-based data storage and analytics.  
- Visualization with **Power BI** dashboards.  
- Predictive modeling for data-driven logistics optimization.  

---

## 🧩 Tech Stack
- **Python, PySpark, Apache Airflow**
- **Storj (S3-Compatible Storage)**
- **Snowflake**
- **Power BI**
- **FastAPI**
- **Faker (for synthetic data generation)**

---

## 📂 Project Architecture
![Blank diagram](https://github.com/user-attachments/assets/6957701d-506d-4cd7-ac56-8c85004b1c71)

