# 🔄 Secure Fin-ETL: Banking Data Middleware

![Status](https://img.shields.io/badge/Status-Production_Enterprise-success)
![Type](https://img.shields.io/badge/Type-Data_Engineering_Pipeline-blue)
![Stack](https://img.shields.io/badge/Stack-Python_|_Oracle_|_MSSQL_|_Mongo-orange)

> **⚠️ Portfolio Notice:** This repository is a technical overview of the **Secure ETL Middleware** developed for Adroit Consulting. The source code is proprietary. This document demonstrates my expertise in building high-volume data pipelines for financial institutions.

---

## 🏗️ Project Overview

In the banking sector, "Core Banking Applications" (like Finacle or Flexcube) are often isolated from "Compliance & Risk Servers." Moving data between them for analysis usually requires expensive, proprietary tools that are slow and rigid.

**Secure Fin-ETL** is a custom-built Middleware designed to replace those expensive tools. It securely **Extracts** transaction logs, **Transforms** them into compliance-ready formats, and **Loads** them into our Fraud Detection Systems (iConcept4Pro).



[Image of ETL data pipeline architecture diagram]


---

## ⚡ Key Highlight: Parallel Data Migration Engine

The core innovation of this software is its ability to handle **Heterogeneous Parallel Migrations**.

Standard ETL tools often run sequentially (Source A -> Dest A, then Source B -> Dest B). I engineered this system to handle concurrent data streams from completely different database technologies simultaneously without locking the production database.

### How it Works
1.  **Multi-Source Ingestion:** The system can open simultaneous connection pools to:
    * **Oracle DB** (Legacy Core Banking Data)
    * **MSSQL** (Card & ATM Transactions)
    * **MongoDB** (Unstructured App Logs)
    * **Flat Files** (CSV/Excel EOD Reports)
2.  **Asynchronous Processing:** Using Python's multiprocessing capabilities, the system extracts data chunks from these sources in parallel threads.
3.  **Unified Transformation Layer:** Regardless of the source (SQL or NoSQL), data is normalized into a standard Python object structure before being loaded into the destination.

> **Impact:** This reduced the "End of Day" (EOD) data migration window from **4 hours to 45 minutes**, allowing Compliance Officers to start their work earlier every morning.

---

## 🛡️ System Features

### 1. Embedded Python Editor for Transformation
* **Feature:** Integrated a code editor within the React dashboard.
* **Utility:** Allows Senior Data Engineers to write custom Python transformation scripts (e.g., `def clean_currency(value): return ...`) directly in the UI to handle complex data edge cases without redeploying the software.

### 2. Real-Time Data Dashboard
* **Frontend:** Built with `React.js`.
* **Visuals:** Shows live progress bars for migration jobs, distinct row counts (Source vs. Destination), and error logs in real-time.

### 3. Secure Transport
* **Encryption:** Implements strict encryption for data in transit to ensure customer PII (Personally Identifiable Information) is never exposed during migration.

---

## 🛠️ Tech Stack

* **Frontend:** React.js (Dashboard & Monitoring).
* **Backend Engine:** Python (Django & Pandas).
* **Database Drivers:** `cx_Oracle` (Oracle), `pyodbc` (MSSQL), `pymongo` (MongoDB).
* **Queue Management:** Redis & Celery (for managing job pipelines).

---

## 👨‍💻 My Role

As the **Lead Architect**, I was responsible for:
1.  **Driver Optimization:** Tuning the Oracle and MSSQL database drivers to prevent timeouts during massive data fetches (1M+ rows).
2.  **Algorithm Design:** Writing the logic that balances load across the server CPU cores during parallel processing.
3.  **Cost Reduction:** This custom solution saved the company significant licensing fees by removing reliance on third-party enterprise ETL tools.

---

## 📬 Contact

**Tunde [Last Name]**
*Senior Full Stack Developer & Data Engineer*
[Link to LinkedIn] | [Link to Portfolio]
