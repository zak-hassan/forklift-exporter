# Forklift CLI	
Cli Application for doing bulk exports of data from source to sink:

Currently we have local file to s3 integration but I plan on including the below components as locations to fetch files or store files


Components Planned To Be Included:

- local filesystem
- s3
- hdfs
- http
- activemq
- kafka
- ftp
- elasticsearch
- mongodb
- gluster
- cassandra
- ceph
- mongo
- postgres
- hive
- mysql
- influx
- apache ignite


# Usage

```bash

cd forklift-cli

mvn clean install

java -jar target/forklift-1.0-SNAPSHOT.jar -source file://$PWD/src/main/resources/orders.json?dataformat=json -sink s3://analyticsio-sandbox/demo1/halloffame.parquet?region=us-east-2&dataformat=parquet

```

# Components

By default the input and output dataformat will be json. If you would like to convert from json to parquet you will need to provide a schema in avro for the conversion to happen. You can provide a local file or a url.



- required input: 
	* -source 	: location of the json input file
	* -sink 	: location will write data to.
	* -avroschema 	: location where the avro schema is stored (optional)

Note: For source/sink input you must provide a prefix type of component being used for example s3:// for storing or fetching from s3 and file:// for local filesystem, etc.


- Not ready for usage still in development



# Demo

[![ScreenShot](https://raw.githubusercontent.com/zmhassan/kafka-s3-exporter/master/imgs/S3VimeoParquet.png)](https://vimeo.com/207705541)

