# Modern-Data-Engineer
Harnessing the incredible power of Apache Spark to orchestrate consistent scalable streaming applications built from the fundamental building blocks of the modern data ecosystem, by you, the modern data engineer.


## Overview
- This repository is inspired by - https://github.com/newfront/spark-moderndataengineering - Scott Haines - Modern Data Engineering with Apache Spark
- I decided to create this repository to help me learn Apache Spark and to help others learn Apache Spark.
- I also wanted to create a repository that is more up to date with the latest versions of Apache Spark and the latest versions of the tools used in the modern data ecosystem.
- Additionally, we would be making use of Jupiter notebooks to help us learn Apache Spark. (not Zepplin as Scott did in his version).

## Caveats
- This repository is a work in progress and is not complete.
- The contents of this repo are written with macOS and Linux systems in mind.

---
# Tech Stack

| Technology | Version | Documentation |
|------------|---------|---------------|
| Docker | Latest | [Documentation](https://docs.docker.com/) |
| Gitpod | Latest | [Documentation](https://www.gitpod.io/docs/introduction/getting-started) |
| Apache Spark | 3.2.0 | [Documentation](https://spark.apache.org/docs/3.2.0/) |
| Apache Hadoop | 3.2 | [Documentation](https://spark.apache.org/docs/3.2.0/) |
| Apache Kafka | 2.8.1 | [Documentation](https://kafka.apache.org/documentation/) |
| Java OpenJDK | 11 | [Documentation](https://docs.oracle.com/en/java/javase/11/) |


---

# Volume 0:  
- I : [Getting Started with Apache Spark ](https://medium.com/@le.oasis/getting-started-with-apache-spark-sparksql-scala-with-mac-terminal-b9c9513c51f1)

Gain hands-on experience with Apache Spark using the spark-shell by delving into my comprehensive [Medium article](https://medium.com/@le.oasis/getting-started-with-apache-spark-sparksql-scala-with-mac-terminal-b9c9513c51f1). Discover the intricacies of Spark and learn how to write, test, and deploy Spark applications with ease. 

---
# [Volume 1](https://github.com/le-oasis/Modern-Data-Engineer/tree/main/vol-01): 
This volume will guide you through running Jupyter Notebook on Docker and using Spark to read and process raw text files.
- You will learn about the structure of DataFrames and how to work with schemas to make data engineering tasks more efficient and streamlined.
- The overarching goal is to help you leverage Spark to become a more efficient and effective data engineer.
Throughout the exercise, we will be using the Spark SQL and DataFrames documentation as a reference to ensure we're following best practices and getting the most out of the tools.
Specifically, you will learn about:
- Running Jupyter Notebook on Docker
- Reading and processing raw text files using Spark
- Understanding the structure of DataFrames and how to work with schemas
- Making data engineering tasks easier and more efficient with Spark
- Leveraging the power of Spark to become a better data engineer

#### Jupyter Dockerfile: Build the Image.
- This Dockerfile sets up an environment for working with Apache Spark, a distributed computing system used for processing large datasets. The image is based on a pre-existing image called `scipy-notebook` that includes many popular data science packages.

- The Dockerfile installs `OpenJDK`, a package that includes the `Java Runtime(11)` Environment, and then downloads and installs `Spark (3.2.0)` . It sets environment variables for Spark and Hadoop`3.2` (another distributed computing system that Spark can run on), and sets the working directory to the user's home directory.

- It then installs Python packages listed in a file called `requirements.txt`  and sets the permissions for the Conda directory and the user's home directory. 

- Finally, it exposes ports 4040, 8088, and 8089 for the container to use.

- Overall, this `scipy-notebook` Dockerfile provides a simple way to set up a working environment for working with Apache Spark in Python.


---
# [Volume 2](https://github.com/le-oasis/Modern-Data-Engineer/tree/main/vol-02):  
- The previous volume introduced the concepts of Docker and Jupyter Lab as tools for conducting Spark explorations.
- In the current volume, we will dive deeper into data transformation techniques using Spark Specifically, we will cover the following topics:
  - Simple selection and projection techniques for working with data subsets
  - Filtering data based on specific criteria
  - Joining datasets together to create a unified view of the data
  - Aggregating data to create summary statistics and metrics

Our focus will be on transforming loosely structured data into highly structured data by using explicit schemas and an ETL (Extract, Transform, Load) job.
- This process is the first step in a data transformation pipeline, as data ingestion often begins at the data lake.
- We will be using the Spark SQL and DataFrame APIs to express these transformations, which are powerful tools for data manipulation and transformation.


# [Volume 3](https://github.com/le-oasis/Modern-Data-Engineer/tree/main/vol-03): 

- Learn to read and write data easily between Spark and any JDBC-compatible RDBMS database
Databases that are compatible with JDBC include MySQL, PostgreSQL, Microsoft SQL Server, Azure SQL Database, Oracle, and others.
- Gain the ability to load and transform data from external database rows into Spark DataFrames.
- Write the modified data back to the original database, which is known as the "source-of-truth" database.
- evelop a seamless workflow for exchanging data between Spark and various RDBMS databases.

