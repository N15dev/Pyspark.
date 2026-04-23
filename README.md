# PySpark Practice - Transformation 

 basic PySpark DataFrame operations implemented in Databricks.

It covers:
- DataFrame creation
- Schema understanding
- Column selection
- Filtering data
- Lazy evaluation concept

 # PySpark: withColumn + CASE WHEN

## Problem
Add a new column `salary_flag` based on salary condition:
- If salary > 80000 → "High"
- Else → "Low"

---

## Approach
- Use `withColumn()` to create a new column
- Use `when()` and `otherwise()` for conditional logic
- Similar to SQL `CASE WHEN`

---

## PySpark Solution

```python
from pyspark.sql.functions import when

df = df.withColumn(
    "salary_flag",
    when(df.salary > 80000, "High").otherwise("Low")
)
