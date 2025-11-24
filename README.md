# 🔄 Secure Fin-ETL: Banking Data Middleware (Automation Bot)

![Status](https://img.shields.io/badge/Status-Production_Engine-success)
![Type](https://img.shields.io/badge/Type-Python_Automation_Script-blue)
![Stack](https://img.shields.io/badge/Stack-Python_Multiprocessing_|_Oracle_|_Pandas-orange)

> **⚠️ Source Code Notice:** This repository contains the **Core Python ETL Bot** (the migration engine). The associated React Dashboard and Django API orchestration layers are proprietary property of Adroit Consulting and are not included. This code demonstrates the **Data Engineering logic** used to move high-volume financial data.

---

## 🏗️ Project Overview

In the banking sector, "Core Banking Applications" (like Finacle or Flexcube) are often isolated from "Compliance & Risk Servers." Moving data between them for analysis usually requires expensive, proprietary tools that are slow and rigid.

**Secure Fin-ETL** is a custom-built Middleware designed to replace those expensive tools. It securely **Extracts** transaction logs, **Transforms** them into compliance-ready formats, and **Loads** them into our Fraud Detection Systems (iConcept4Pro).

<div align="center">
  <img src="./assets/2025-11-24 04_49_59-C__Users_HP_Desktop_secure-fin-etl_README.md (afro-ai, photo2calendar, campus ca.png" alt="ETL Data Pipeline Architecture" width="800">
  <p><em>Figure 1: Data Migration Pipeline Connectors Configuration</em></p>
</div>

---

## ⚡ Key Highlight: Parallel Data Migration Engine

The core innovation of this script is its ability to handle **Heterogeneous Parallel Migrations**.

Standard ETL tools often run sequentially. I engineered this bot to handle concurrent data streams from completely different database technologies simultaneously without locking the production database.

### How the Script Works
1.  **Multi-Source Ingestion:** The bot opens simultaneous connection pools to:
    * **Oracle DB** (Legacy Core Banking Data)
    * **MSSQL** (Card & ATM Transactions)
    * **MongoDB** (Unstructured App Logs)
    * **Flat Files** (CSV/Excel EOD Reports)
2.  **Asynchronous Processing:** Using Python's `multiprocessing` library, the script extracts data chunks from these sources in parallel threads.
3.  **Unified Transformation Layer:** Regardless of the source (SQL or NoSQL), data is normalized into a standard Python dictionary structure before being loaded into the destination.

> **Impact:** This reduced the "End of Day" (EOD) data migration window from **4 hours to 45 minutes**, allowing Compliance Officers to start their work earlier every morning.

---

## 📂 Repository Structure (The Bot)

This repository focuses on the backend logic:

```text
├── etl_engine/
│   ├── connectors/         # Custom drivers for Oracle, MSSQL, Mongo
│   ├── transformers/       # Logic to clean currency, dates, and PII
│   ├── loaders/            # Bulk insert logic for destination DB
│   └── main_bot.py         # The entry point for the migration job
└── utils/
    └── encryption.py       # PII masking utilities