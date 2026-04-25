# 🚀 ETL Pipeline with Incremental Load (Delta + Parquet)

## 📌 Overview

This project demonstrates a PySpark-based ETL pipeline that loads data incrementally from MSSQL into Delta/Parquet storage using modification timestamps.

---

## ⚙️ Tech Stack

* Python (PySpark)
* Delta Lake
* Parquet
* MSSQL

---

## 🔄 Pipeline Logic

* Extract data from MSSQL
* Filter new/updated records using `ModifiedDate`
* Load data into Delta using MERGE (UPSERT)

---

## 🔁 Incremental Load (MERGE)

```python
deltaTable.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

✔ Updates existing records
✔ Inserts new records

---

## 🎯 Key Features

* Incremental loading (no full reload)
* Delta Lake MERGE (UPSERT)
* Scalable ETL design

---

## 📂 Project Structure

```
/project
 ├── extraction/
 ├── transformation/
 ├── load/
```

---

## 🚀 Future Improvements

* Delete handling (full CDC)
* Medallion Architecture (Bronze/Silver/Gold)
* Airflow orchestration

---

## 👤 Author

Giorgi Megeneishvili
