# Volume 2:  
- The previous volume introduced Docker and Jupyter Lab for Spark explorations. 
- It focused on transforming loosely structured data to highly structured data using explicit schemas and an ETL job. 
- This process is the first step in a data transformation pipeline because data ingestion often begins at the data lake.
- The current volume explores data transformation through simple selection and projection techniques, joins, and intuitive problem-solving methods like inner and outer selections, expression columns, and select expressions. 
- The Spark SQL and DataFrame APIs can be used to express these transformations.

---
## Workshop Material

Setting up your environment is covered in Volume 1, but as long as you have the following set in your local env `bash or zsh`, you will be golden.
1. JAVA_HOME - (must be java 8 or java 11)
2. SPARK_HOME - (we are using spark 3x)


Start the Docker Container
~~~
cd  volume-two/start/docker/ && ./run.sh start
~~~

Go to http://localhost:8888 to see the jupyter lab homepage.




