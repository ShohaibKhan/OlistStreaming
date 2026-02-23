# OlistStreaming  
*A Real-Time E-Commerce Streaming Data Pipeline on AWS*

---

## About This Project

This project was built to simulate how modern data platforms handle streaming data in production environments.

Instead of simply loading static CSV files into a warehouse, this pipeline:

- Simulates real-time e-commerce events  
- Processes them using Apache Spark  
- Stores raw and processed data in Amazon S3  
- Automatically ingests data into Amazon Redshift  
- Models the warehouse using a Star Schema  
- Enables analytics-ready business queries  

The dataset used is the Olist Brazilian e-commerce dataset. The goal was not just analysis — but designing a complete, scalable data pipeline.

---

## What This Project Demonstrates

- Streaming-style ingestion  
- Data lake architecture (Raw + Processed zones)  
- Spark-based transformations  
- Incremental loading into Redshift  
- Star schema data modeling  
- Fact & dimension separation  
- Partitioning strategy  
- Warehouse optimization concepts  

This is designed as an end-to-end data engineering project.

---

## Architecture Overview

```
![alt text](https://github.com/ShohaibKhan/OlistStreaming/blob/main/aws_architecture.drawio.png?raw=true)
```

---

## Architecture Layers

### Event Producer

A Python-based producer simulates streaming events from historical Olist data.

Instead of bulk loading everything at once, it emits data in smaller batches to mimic real-time ingestion patterns.

---

### Spark Streaming (EMR)

The Spark application:

- Reads incoming events  
- Applies schema validation  
- Deduplicates records  
- Calculates derived metrics (delivery delays, etc.)  
- Writes partitioned Parquet files to S3  

This transforms raw event data into structured, analytics-ready datasets.

---

### S3 Data Lake

Two logical layers:

#### Raw Layer
- Stores unprocessed event data  
- Useful for replay and debugging  

#### Processed Layer
- Cleaned and structured Parquet files  
- Partitioned by time  
- Optimized for warehouse ingestion  

---

### Redshift Staging Layer

Staging tables mirror the processed S3 schema.

Auto COPY jobs continuously ingest new data from S3 without manual execution:

This simulates incremental style ingestion.

---

### Data Warehouse (Star Schema)

The warehouse layer follows a Star Schema model.

#### fact_orders

#### dim_customers  
#### dim_products  
#### dim_payments  
#### dim_date  

The fact table sits at the center and connects to dimension tables for analytical slicing.

## ⭐ Final Note

This project reflects a practical approach to building scalable data systems:

- Clear separation of layers  
- Incremental ingestion strategy  
- Cloud-native architecture mindset  

If you're reviewing this repository for collaboration or hiring, I’d be happy to walk through the design decisions behind each layer.
