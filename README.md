# kafka-s3-exporter
Exporting kafka messages in bulk into a single file on s3. I will probably have other storage options later.

# Usage

```bash
cd forklift-cli

mvn clean install

java -jar target/forklift-1.0-SNAPSHOT.jar -source file://$PWD/src/main/resources/orders.json?dataformat=json -sink s3://analyticsio-sandbox/demo1/halloffame.parquet?region=us-east-2&dataformat=parquet

```

- Note: Need to add more logic to upload parquet to s3.

- Not ready for usage still in development

- required input: 
	* -input 	: location of the json file that will be converted to parquet
	* -output 	: location of output parquet file.
	* -s3filename 	: location where the output file will get stored at.
# Components

By default the input and output dataformat will be json. If you would like to convert from json to parquet you will need to provide a schema in avro for the conversion to happen. You can provide a local file or a url.

Components:

- local filesystem
- http
- hdfs
- s3
- activemq
- kafka
- ftp
- twitter
- elasticsearch
- mongodb
- gluster
- cassandra
- ceph



# Demo

[![ScreenShot](https://raw.githubusercontent.com/zmhassan/kafka-s3-exporter/master/imgs/S3VimeoParquet.png)](https://vimeo.com/207705541)

