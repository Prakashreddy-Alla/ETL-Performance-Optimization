# 🚀 Oracle ETL Performance Tuning: Over 54% Time Reduction

## Project Summary

This project demonstrates expertise in optimizing high-volume ETL (Extract, Transform, Load) processes within an Oracle Database environment. The core objective was to replace an inefficient, **row-by-row PL/SQL procedure** with a high-performance **set-based SQL solution** to drastically reduce execution time.

### **Key Result**
Reduced ETL execution time from **1.377 seconds** to **0.632 seconds**, achieving a performance gain of **over 54.0%**.

---

## 💡 The Problem: Row-by-Row Inefficiency (The Baseline)

The initial procedure (`PRC_ETL_SALES_BASELINE`) was designed to aggregate daily sales from the high-volume `SALES_TRANSACTIONS` table.

* **Approach:** It used a **PL/SQL cursor loop** to process each transaction.
* **Bottleneck:** This approach forced the database to perform over 32,000 costly **context switches** between the slow PL/SQL engine and the fast SQL engine. 
* **Metric:** Baseline Time = **1.377 seconds**.

## ✨ The Solution: Set-Based Optimization (The Fix)

The optimized procedure (`PRC_ETL_SALES_TUNED`) was refactored to fully leverage Oracle's set-based capabilities.

* **Approach:** Replaced the procedural loop with a single, highly efficient **`MERGE` statement**.
* **Functionality:** The `MERGE` statement's source subquery performed all the necessary aggregation (`GROUP BY`) on the **32,525 transactions**, reducing the target write operations to just **4 summary records**.
* **Benefit:** This eliminated context switching, delegating the heavy computation to the highly optimized SQL engine.

## 📊 Quantifiable Results

| Metric | Baseline Procedure (`PRC_ETL_SALES_BASELINE`) | Tuned Procedure (`PRC_ETL_SALES_TUNED`) | Improvement |
| :--- | :--- | :--- | :--- |
| **Processing Time** | **1.377 seconds** | **0.632 seconds** | **54.0% Faster** |
| **Load Operations (MERGE Executions)** | 32,525 (in a loop) | 1 (single execution) | Massive reduction in overhead |
| **Workload Size (Source Rows)** | 32,525 | 32,525 | (Identical Workload) |
