# Volume 3:  
This chapter is divided into three main sections and exercises:

A crash course on PostgreSQL on Docker.
Connecting the RDBMS world with Spark SQL using JDBC.
Additional exercises.
Let's get started by setting up our development environments and diving into these topics!

---
## Workshop Material

### 1. Setting up PostgreSQL on Docker
#### File Location:
Navigate to the `vol-03` directory, this is where the `docker-compose.yml` file is located:

- `start` --> `docker` --> `docker-compose.yml`.



#### PostgreSQL Service.
This is a service configuration specified in a docker-compose.yml file.

![](../doc/postservice.png "Postgres-Docker")

- The service is named `postgres` and will be used to run a Postgres database.
- The hostname of the container will be set to `mypostgres`.
- The container will be given the name `postgres`.
- The image for the container will be built using a Dockerfile located in the ./lakehouse/postgres directory.
Two volumes will be mounted to the container:
- A volume named `postgres-db-volume` will be mounted to the /var/lib/postgresql/data directory in the container. This is where Postgres will store its data.
- A file named `create_ranger_users.sql` located in the ./lakehouse/postgres directory will be mounted to the /docker-entrypoint-initdb.d directory in the container. This file will be executed when the container is started and will create some Ranger users in the Postgres database.
- An environment file named `.env` will be used to set environment variables in the container.
- The container will expose port 5432, which is the default port for Postgres.
- A health check will be performed on the container every 5 seconds using the "pg_isready" command to ensure that the container is healthy.
- The container will be restarted automatically if it fails or is stopped.
- The container will be connected to a network named `oasiscorp`.


### 2. Setting up your environment

Setting up your environment is covered in Volume 1, but as long as you have the following set in your local env `bash or zsh`, you will be golden.
1. JAVA_HOME - (must be java 8 or java 11)
2. SPARK_HOME - (we are using spark 3x)


Start the Docker Container
~~~
cd  volume-two/start/docker/ && ./run.sh start
~~~

Go to http://localhost:8888 to see the jupyter lab homepage.

---
* For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. 
* The URL with the token can be taken from container logs using:
 
```
docker logs $(docker ps -q --filter "ancestor=docker-jupyter") 2>&1 | grep 'http://127.0.0.1' | tail -1
```
---



