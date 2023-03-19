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
- This chapter builds on the previous one by exploring the process of reading, transforming, and writing structured data through the DataFrame API and Spark SQL.
- In this exercise, you will learn how to run Jupyter Notebook on Docker and use Spark to read and process raw text files. 
- You will learn about the structure of DataFrames and how to work with schemas to make data engineering tasks easier and more efficient. 
- The goal is to help you leverage Spark to become a more efficient data engineer.
- We will be using the [Spark SQL and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html) documentation as a reference.


#### Jupyter Dockerfile: Build the Image.
- This Dockerfile sets up an environment for working with Apache Spark, a distributed computing system used for processing large datasets. The image is based on a pre-existing image called `scipy-notebook` that includes many popular data science packages.

- The Dockerfile installs `OpenJDK`, a package that includes the `Java Runtime(11)` Environment, and then downloads and installs `Spark (3.2.0)` . It sets environment variables for Spark and Hadoop`3.2` (another distributed computing system that Spark can run on), and sets the working directory to the user's home directory.

- It then installs Python packages listed in a file called `requirements.txt`  and sets the permissions for the Conda directory and the user's home directory. 

- Finally, it exposes ports 4040, 8088, and 8089 for the container to use.

- Overall, this `scipy-notebook` Dockerfile provides a simple way to set up a working environment for working with Apache Spark in Python.

#### File Location:
Navigate to the `jupyter` directory, this is where the `Dockerfile` is located:

- `start` --> `docker` --> `lakehouse`  --> `jupyter` --> `Dockerfile`.
- It will take about ***10minutes*** to build, depending on yor internet speed / platform you use to build the image.

```
docker build --rm --force-rm -t oasis-jupyter:latest . 
```


#### Running Jupyter Lab with Docker
##### `run.sh`

- Execute the following two simple steps to start the Jupyter process. This process will install and then start up Jupyter. 
- If this is the first time installing the Jupyter Docker image, it will take a few minutes depending on your Internet connection. Once this Docker image is cached locally, starting and stopping the Jupyter environment will take no time at all.
1.   cd volume-one/start/docker/
2.   ./run.sh start

- At this point you will have Jupyterlab running on your laptop.

- Peeking at the `volume-one/start/docker/run.sh` script gives you more complete details regarding how the environment is set up. 

- The start Function in the `run.sh` Script Ensures Your Local Environment Is Set Up As Expected, then Delegates the Actual Running of the Jupyter Container to the Docker Compose Process.

    
        function start() {
                
                createNetwork
                docker-compose -f ${DOCKER_COMPOSE_FILE} up --build -d --remove-orphans jupyter
                echo "Jupyter will be running on http://127.0.0.1:8888"
        }



##### `docker compose`
- We will be using the `docker-compose.yml` file to run the image.

After building the dockerfile & pulling the necessary images, you're ready to rock n roll. 
- Navigate to the `docker` directory:
- Run the following command to start the services:

~~~
docker compose -f docker-compose.yml up --build -d
~~~

To ensure the services are running, you can click on the following URLs:

#### Jupyter: http://localhost:8888

* For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. 
* The URL with the token can be taken from container logs using:
 
```
docker logs $(docker ps -q --filter "ancestor=docker-jupyter") 2>&1 | grep 'http://127.0.0.1' | tail -1
```

---
# [Volume 2](https://github.com/le-oasis/Modern-Data-Engineer/tree/main/vol-02):  
- The previous volume introduced Docker and Jupyter Lab for Spark explorations. 
- The current volume explores data transformation through: simple selection and projection techniques, filtering, joins and aggregations.
- It focused on transforming loosely structured data to highly structured data using explicit schemas and an ETL job. 
- This process is the first step in a data transformation pipeline because data ingestion often begins at the data lake.
- The Spark SQL and DataFrame APIs can be used to express these transformations.


# [Volume 3](https://github.com/le-oasis/Modern-Data-Engineer/tree/main/vol-03): 
- Learn to read and write data easily between Spark and any JDBC-compatible RDBMS database
Databases that are compatible with JDBC include MySQL, PostgreSQL, Microsoft SQL Server, Azure SQL Database, Oracle, and others.
- Gain the ability to load and transform data from external database rows into Spark DataFrames.
- Write the modified data back to the original database, which is known as the "source-of-truth" database.
- evelop a seamless workflow for exchanging data between Spark and various RDBMS databases.

