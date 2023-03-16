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


### Setting Up Jupyter Notebook 
- We will be using Jupyter Notebook to run our Spark code.

#### Dockerfile: Build the Image.
- This Dockerfile sets up an environment for working with Apache Spark, a distributed computing system used for processing large datasets. The image is based on a pre-existing image called `scipy-notebook` that includes many popular data science packages.

- The Dockerfile installs `OpenJDK` , a package that includes the `Java Runtime(11)` Environment, and then downloads and installs `Spark (3.2.0)` . It sets environment variables for Spark and Hadoop`3.2` (another distributed computing system that Spark can run on), and sets the working directory to the user's home directory.

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



