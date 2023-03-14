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

## Pre-requisites

Docker: Docker is a set of platform as a service (PaaS) products that use OS-level virtualization to deliver software in packages called containers.

- Docker Image: A Docker image is a file used to execute code in a Docker container. Docker Images act as a set of instructions to build a Docker container, like a template. Docker Images also act as the starting point when using Docker. 

    * An image resembles a snapshot in virtual machine (VM) environments.

- Dockerfile: A Dockerfile is a text document that contains all the commands a user could call on the command line to assemble an image.

- Docker Compose: A tool for defining and running multi-container Docker applications.

    - With docker-compose, we can create a YAML file to define the services and, with a single command, spin everything up or tear it all down.
    - This process's most significant pain points revolve around consistency and runtime guarantees. 
    - Often, what works locally for you may operate with a different level of finesse (or at all) for other engineers (so beware; there might/will be bugs). 

Container: A container is a standard unit of software that packages up code and all its dependencies so the application runs quickly and reliably from one computing environment to another.

Gitpod: Gitpod is an open-source platform for automated and ready-to-code development environments that blends into the existing dev workflow.

    - We will use Gitpod as our development environment. 
    - It ships with a built-in Docker instance to run docker-compose commands in the cloud.

- YAML: YAML is a powerful language that can be used for configuration files, messages between applications, and saving application state.

---
# Tech Stack

| Technology | Version | Documentation |
|------------|---------|---------------|
| Docker | Latest | [Documentation](https://docs.docker.com/) |
| Gitpod | Latest | [Documentation](https://www.gitpod.io/docs/introduction/getting-started) |
| Apache Spark | 3.2.0 | [Documentation](https://spark.apache.org/docs/3.2.0/) |
| Apache Hadoop | 3.2 | [Documentation](https://spark.apache.org/docs/3.2.0/) |
| Apache Kafka | 2.8.1 | [Documentation](https://kafka.apache.org/documentation/) |


---

# Volume 0:  
- I : [Getting Started with Apache Spark ](https://medium.com/@le.oasis/getting-started-with-apache-spark-sparksql-scala-with-mac-terminal-b9c9513c51f1)

Gain hands-on experience with Apache Spark using the spark-shell by delving into my comprehensive [Medium article](https://medium.com/@le.oasis/getting-started-with-apache-spark-sparksql-scala-with-mac-terminal-b9c9513c51f1). Discover the intricacies of Spark and learn how to write, test, and deploy Spark applications with ease. 

---
# Volume 1:  
- This chapter builds on the previous one by exploring the process of reading, transforming, and writing structured data through the DataFrame API and Spark SQL.
- In this exercise, you will learn how to run Jupyter Notebook on Docker and use Spark to read and process raw text files. 
- You will learn about the structure of DataFrames and how to work with schemas to make data engineering tasks easier and more efficient. 
- The goal is to help you leverage Spark to become a more efficient data engineer.
- We will be using the [Spark SQL and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html) documentation as a reference.


## Setting Up Jupyter Notebook 
- We will be using Jupyter Notebook to run our Spark code.

### Dockerfile: Build the Image.
- This Dockerfile sets up an environment for working with Apache Spark, a distributed computing system used for processing large datasets. The image is based on a pre-existing image called `scipy-notebook` that includes many popular data science packages.

- The Dockerfile installs `OpenJDK` , a package that includes the `Java Runtime(11)` Environment, and then downloads and installs `Spark (3.2.0)` . It sets environment variables for Spark and Hadoop`3.2` (another distributed computing system that Spark can run on), and sets the working directory to the user's home directory.

- It then installs Python packages listed in a file called `requirements.txt`  and sets the permissions for the Conda directory and the user's home directory. 

- Finally, it exposes ports 4040, 8088, and 8089 for the container to use.

- Overall, this `scipy-notebook` Dockerfile provides a simple way to set up a working environment for working with Apache Spark in Python.

### File Location:
Navigate to the `jupyter` directory, this is where the `Dockerfile` is located:

- `start` --> `docker` --> `lakehouse`  --> `jupyter` --> `Dockerfile`.
- It will take about ***10minutes*** to build, depending on yor internet speed / platform you use to build the image.

```
docker build --rm --force-rm -t oasis-jupyter:latest . 
```







