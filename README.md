# Adventure Works: End-to-End Data Engineering Pipeline in Azure & Power BI

<div align="center">
  <img src="https://github.com/user-attachments/assets/9660a7e8-59df-4d1f-a43a-c3ddc3781b22" alt="Microsoft Azure Logo" width="200">
</div>

## Overview

This project implements a **production-grade data pipeline** on Microsoft Azure, transforming raw sales data from Adventure Works into actionable business intelligence through a **medallion architecture**. The solution delivers a self-service Power BI dashboard powered by an optimized star schema model.

## Solution Architecture

### High-Level Design
![Medallion Architecture Pipeline](https://github.com/user-attachments/assets/3325e7c7-0e3f-443b-b24c-e059248d55fa)  
*Visualized with Lucidchart*

## Data Acquisition

**Initial Approach:**
- Attempted to restore AdventureWorks `.bak` file using SQL Edge in Docker (Mac compatibility solution)

**Optimized Solution:**
- Discovered pre-processed CSV datasets in GitHub
- Implemented **dynamic ADF pipeline** with GitHub linked service for automated data ingestion  
- 🔗 [Source Datasets](https://github.com/AbdulRafay365/End-to-End-Data-Engineering-Pipeline-in-Azure-and-Power-BI/tree/main/Datasets/Data)

## Technical Implementation

### Tech Stack
| Category        | Technologies Used                     |
|-----------------|---------------------------------------|
| **Data Storage** | Azure SQL, Data Lake Gen2            |
| **Processing**  | Azure Data Factory, Databricks (PySpark), Synapse Analytics |
| **Visualization** | Power BI                            |
| **Infrastructure** | Azure Active Directory, Docker      |
| **Design**      | Lucidchart                          |

### Medallion Architecture Implementation

#### Bronze Layer (Raw)
- **ADF Pipeline**: Ingested raw CSV files with schema-on-read
![Bronze Layer Pipeline](https://github.com/user-attachments/assets/8c3fa954-969e-44c6-9051-c25b609646ff)

#### Silver Layer (Cleansed)
- Performed data type conversions
- Handled NULL values and data quality checks
- Established entity relationships
- Implemented normalization
![Silver Layer Transformations](https://github.com/user-attachments/assets/44d257c0-b4c7-40dc-a5f5-89e2b7d3090d)
![Data Validation](https://github.com/user-attachments/assets/07f4065d-c693-42ed-b0e6-ddfd3d58c70f)

#### Gold Layer (Business Ready)
- Created star schema dimensional model
- Built optimized views for analytics
![Star Schema Model](https://github.com/user-attachments/assets/7daf7cb3-8d90-4c4b-8c3f-770940198024)

## Business Intelligence Delivery

### Power BI Dashboard
![Interactive Sales Dashboard](https://github.com/user-attachments/assets/cea8a7a1-4418-4a1c-8c36-a5faf319bc1f)

🔗 [Live Dashboard Access](https://app.powerbi.com/groups/me/reports/50ec7294-4eb3-4b1c-95b6-0263fc947780/8c6c6c64e634a40d29d9?experience=power-bi&bookmarkGuid=607f88eba09b02029605)
