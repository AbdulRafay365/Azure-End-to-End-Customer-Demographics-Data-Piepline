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

## Medallion Architecture Implementation

### Bronze Layer Implementation: Dynamic GitHub Data Ingestion
#### Architecture Components

``` mermaid
graph TD
    A[GitHub Raw CSV] -->|Linked Service| B(ADF)
    B -->|Parameterized| C[Lookup Activity]
    C -->|Dynamic Values| D[ForEach Loop]
    D -->|Per File| E[Copy Activity]
    E --> F[Data Lake Raw Zone]
```

**1) Linked Services Configuration**
   
1.1 GitHub Linked Service (githublinkedservice)

``` json
{
    "name": "githublinkedservice",
    "type": "HttpServer",
    "typeProperties": {
        "url": "https://raw.githubusercontent.com/",
        "authenticationType": "Anonymous"
    }
}
```

* Purpose: Establishes connection to GitHub's raw content domain
* Key Feature: Anonymous access to public repositories
* Security: No credentials needed (public data)

1.2 Data Lake Linked Service (storagedatalake)

```json
{
    "name": "storagedatalake",
    "type": "AzureBlobFS",
    "typeProperties": {
        "url": "https://abdulrafayawstorage.dfs.core.windows.net/"
    }
}
```
* Storage Target: Your Azure Data Lake Gen2 account
* Authentication: Uses encrypted credential (automatically handled by ADF)

**2) Parameterized Datasets**
   
2.1 Control Dataset (ds_git_parameters)

```json
{
    "location": {
        "type": "AzureBlobFSLocation",
        "fileName": "git.json",
        "fileSystem": "parameters"
    }
}
```

Contains: JSON file with array of file configurations:

``` json
[
  {
    "p_rel_url": "AbdulRafay365/.../Product.csv",
    "p_sink_folder": "products",
    "p_file_name": "products_raw_20250608.txt"
  },
]
```

Dynamic Fields:
* p_rel_url: GitHub relative path
* p_sink_folder: Target folder in Data Lake
* p_file_name: Output filename

2.2 Source Dataset (ds_git_dynamic)

```json
{
    "parameters": {"p_rel_url": "String"},
    "typeProperties": {
        "location": {
            "relativeUrl": "@dataset().p_rel_url"
        }
    }
}
```

* Dynamic Behavior: Relative URL populated at runtime
* File Format: CSV with first row as header

2.3 Sink Dataset (ds_sink_dynamic)

``` json
{
    "parameters": {
        "p_sink_folder": "String",
        "p_file_name": "String"
    },
    "typeProperties": {
        "location": {
            "folderPath": "@dataset().p_sink_folder",
            "fileName": "@dataset().p_file_name",
            "fileSystem": "rawdata"
        }
    }
}
```

* Bronze Layer Location: rawdata container
* Dynamic Pathing: Folder structure created per file type

**3) Pipeline Execution Flow (gittorawdatadynamic)**
   
3.1 Step 1: Lookup Activity

``` json
{
    "name": "LookupGit",
    "type": "Lookup",
    "dataset": {
        "referenceName": "ds_git_parameters"
    }
}

```
* Action: Reads git.json from parameters container
* Output: Array of file configurations

3.2 Step 2: ForEach Loop

``` json
{
    "name": "ForEachGit",
    "items": "@activity('LookupGit').output.value",
    "activities": [
        {
            "name": "dynamiccopy",
            "type": "Copy",
            "inputs": [{
                "parameters": {
                    "p_rel_url": "@item().p_rel_url"
                }
            }],
            "outputs": [{
                "parameters": {
                    "p_sink_folder": "@item().p_sink_folder",
                    "p_file_name": "@item().p_file_name"
                }
            }]
        }
    ]
}
```

Dynamic Processing:

* Iterates through each file definition
* Passes parameters to copy activity
* Maintains original file structure in Data Lake

3.3 Step 3: Copy Activity

* Source: GitHub file via HTTP (@dataset().p_rel_url)
* Sink: Data Lake with parameterized path
* Transformation: None (pure 1:1 copy for bronze layer)
* File Handling: Converts CSV to TXT (controlled by sink dataset)
* Preserves header row

3.4 Key Dynamic Patterns

Parameter Propagation:

```mermaid
graph LR
    A[Control JSON] --> B(Lookup)
    B --> C{ForEach}
    C -->|p_rel_url| D[Source DS]
    C -->|p_sink_folder| E[Sink DS]
    C -->|p_file_name| E
```

**Runtime Binding:**

* All paths resolved during pipeline execution
* No hardcoded filenames or paths
* Extensible Design:
* New files added by updating git.json
* No pipeline modifications needed

**Bronze Layer Characteristics**

* Data Fidelity: Exact byte-for-byte copy of source
* Metadata Preservation: Original filenames stored in path
* Auditability: Timestamps in filenames (e.g., _20250608)
* Partitioning Ready: Folder structure enables future processing

**This implementation demonstrates a production-grade pattern for:**

* Dynamic source-to-landing workflows
* Metadata-driven pipelines
* Scalable file ingestion patterns
  
**ADF Pipeline**
<div align="center">
  <img src= "https://github.com/user-attachments/assets/8c3fa954-969e-44c6-9051-c25b609646ff" width"500">
</div>

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
