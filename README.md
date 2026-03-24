# 🏥 Hospital Data Pipeline on Snowflake

### 📊 End-to-End Data Engineering Project

---

## 🌟 Project Overview

This project demonstrates a **complete Snowflake-based data pipeline** designed for a hospital system.
It transforms raw operational data into **analytics-ready insights** for better decision-making.

🔍 The pipeline enables:

* 👨‍⚕️ Doctor performance analysis
* 🧑‍🤝‍🧑 Patient visit tracking
* 💰 Billing & revenue insights
* 🏥 Department-level performance

---

## 🏗️ Architecture at a Glance

```
        RAW LAYER
            ↓
     VALIDATED LAYER
            ↓
     CLEAN (CURATED)
            ↓
        KPI OUTPUT
```

### 🔹 RAW Layer

* Stores **unprocessed source data**
* Loaded from CSV files using Snowflake stages
* Includes metadata:

  * `FILE_NAME`
  * `LOAD_TIMESTAMP`

---

### 🔹 VALIDATED Layer

* Applies **data cleaning & transformation**
* Ensures **data quality & consistency**
* Key operations:

  * 📅 Date standardization
  * 🔢 Numeric conversions
  * 🧮 Total charge calculation
  * ❌ Bad data segregation
  * 🔁 Duplicate removal

---

### 🔹 CLEAN (Curated Layer)

* Designed using a **star schema**
* Optimized for reporting and analytics

#### 📌 Dimensions:

* `DIM_PATIENT`
* `DIM_DOCTOR`

#### 📌 Fact:

* `FACT_PATIENT_VISITS`

---

## ⚙️ Core Features

### 📥 Data Ingestion

* CSV files loaded via:

  * Snowflake **STAGE**
  * **FILE FORMAT**
  * `COPY INTO`

---

### 🔄 Data Transformation

* Standardized date formats
* Derived metric:

```
TOTAL_CHARGES = CONSULTATION_FEE + MEDICINE_COST + LAB_COST
```

---

### 🛡️ Data Quality Rules

| Rule                    | Action                       |
| ----------------------- | ---------------------------- |
| Missing Patient ID      | Sent to BAD table            |
| Invalid/NULL Visit Date | Sent to BAD table            |
| Negative Billing        | Marked as INVALID            |
| Duplicate Records       | Removed using `ROW_NUMBER()` |

---

### 🔁 Incremental Processing

* Only new records processed using watermark logic:

```sql
LOAD_TIMESTAMP > MAX(LOAD_TS)
```

---

### 🔍 Deduplication Logic

```sql
ROW_NUMBER() OVER (
    PARTITION BY VISIT_ID
    ORDER BY LOAD_TIMESTAMP DESC
)
```

✔ Keeps latest record
✔ Removes duplicates

---

## 📊 KPI Dashboard Metrics

### 👨‍⚕️ Doctor Productivity

* Total visits handled
* Revenue generated

---

### 🧑‍🤝‍🧑 Patient Visits

* Daily visit trends
* Peak load analysis

---

### 💰 Billing & Revenue

* Average revenue per visit
* High billing detection

---

### 🏥 Department Performance

* Revenue by department
* 🥇 Top 3 departments

---

### 🚨 Alerts

* Flags unusually high billing transactions

---

## 🧠 Design Highlights

### 🔹 Star Schema Modeling

* Improves performance
* Simplifies reporting

---

### 🔹 Surrogate Keys

* Used for efficient joins
* Ensures scalability

---

### 🔹 Modular Validation

* Centralized function for:

  * Date validation
  * Billing checks
  * Data quality rules

---

### 🔹 Layered Architecture

* Clear separation of concerns:

  * RAW → VALIDATED → CLEAN

---

## 🚀 How to Run

1. Create database, schemas, and warehouse
2. Create RAW tables
3. Load CSV files using `COPY INTO`
4. Run validation and merge scripts
5. Populate CLEAN layer
6. Execute KPI queries

---

## 📈 Project Status

| Component        | Status     |
| ---------------- | ---------- |
| Data Ingestion   | ✅ Complete |
| Validation       | ✅ Complete |
| Deduplication    | ✅ Complete |
| Incremental Load | ✅ Complete |
| Data Modeling    | ✅ Complete |
| KPI Reporting    | ✅ Complete |

🎯 **Overall Completion: ~90%**

---

## 🔮 Future Enhancements

* ⚡ Implement Snowflake **Streams & Tasks** (CDC pipelines)
* 🔎 Add lookup validation (patient/doctor existence checks)
* 📊 Build dashboards (Power BI / Tableau)
* 🚨 Add real-time alerting

---

## 💡 Key Learnings

* Designing layered data pipelines
* Handling real-world data quality issues
* Implementing incremental loads
* Building fact-dimension models
* Writing analytical SQL

---

✨ *This project showcases a production-style Snowflake pipeline with strong data engineering fundamentals and analytics capability.*
