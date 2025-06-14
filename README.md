## Prerequisites to Install Apache Spark:
1. Java
2. Python
3, Winutils
4. Spark

### Java (installed Java 11)
1.	Apache Spark is written in Scala, which runs on the Java Virtual Machine (JVM).
2.	Spark needs Java to compile, run its engine, and manage RDDs, Data Frames, etc.
3.	Without Java, you cannot start the Spark Shell or run any Spark job.
4.	Minimum requirement: Java 8 or higher (Java 11 is compatible with Spark 3.5.6).

### Spark (installed Spark 3.5.6)
1.	This is the main framework you are using.
2.	Installing Spark provides the binaries and scripts to launch spark-shell, pyspark, and run jobs locally or on a cluster.

### Python (installed Python 3.13.4)
1.	Spark Shell has multiple entry points:
2.	spark-shell → Scala REPL
3.	pyspark → Python REPL
4.	Python is needed only if you're using pyspark (Python API for Spark).
5.	If you're using just spark-shell (Scala-based), Python is not mandatory.
### Winutils (Windows only)
1.	Spark was originally designed for Linux-based environments (like Hadoop clusters).
2.	On Windows, Spark expects certain Hadoop-related native libraries and file system utilities.
3.	winutils.exe acts as a dummy implementation to let Spark run without throwing permission/file system errors on Windows.
Use case: Without winutils.exe, you might see errors like:
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

Note: After installing Java, Python, Spark, and Winutils, make sure to set the corresponding environment variables (JAVA_HOME, SPARK_HOME, HADOOP_HOME) and update the system PATH to include their /bin directories.
This ensures Spark can run correctly from the command line using spark-shell or pyspark.


### Required Components Overview

| Component              | Purpose                                      | Is It Mandatory?                   | Notes                                                                 |
|------------------------|----------------------------------------------|------------------------------------|-----------------------------------------------------------------------|
| Java (e.g., Java 11)   | Runs the JVM-based Spark engine (Scala/Java) |  Yes                              | Spark is built on Scala, which runs on the JVM                        |
| Spark (e.g., 3.5.6)    | The actual big data engine; provides Spark tools | Yes                           | Needed to run Spark applications                                     |
| Python (e.g., 3.13.4)  | Required only for using PySpark (Python API) |  No (for `spark-shell`),  Yes (for `pyspark`) | Use the correct Python version for compatibility with Spark |
| Winutils               | Mimics Hadoop FS utilities on Windows        | Yes (on Windows only)           | Required to avoid Hadoop-related errors on Windows                    |

##  Dataset Selection

### Dataset Name: Online Retail Dataset  
**Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/online+retail)  
**File Format:** `.csv`  
**Selected From:** Public dataset provided by UCI ML Repository, commonly used in retail analytics, customer segmentation, and RFM (Recency, Frequency, Monetary) analysis.

---

### Description

The **Online Retail dataset** contains transactional data from a UK-based and registered non-store online retail company that primarily sells unique all-occasion gifts. It captures data from **December 2010 to December 2011**, recording purchases made by customers from various countries.

This dataset is widely used for:
- E-commerce behavior analysis
- Data preprocessing practice
- Clustering and customer segmentation
- Market basket analysis and association rule mining

---

### Key Columns

| Column Name   | Description                                    |
|---------------|------------------------------------------------|
| `InvoiceNo`   | Unique identifier for each transaction         |
| `StockCode`   | Product/item code                              |
| `Description` | Name/description of the product                |
| `Quantity`    | Number of items purchased                      |
| `InvoiceDate` | Date and time of the transaction               |
| `UnitPrice`   | Price per unit in GBP                          |
| `CustomerID`  | Unique customer identifier                     |
| `Country`     | Country of the customer                        |





