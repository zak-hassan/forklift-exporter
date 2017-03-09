# kafka-s3-exporter
Exporting kafka messages in bulk into a single file on s3. I will probably have other storage options later.

# Usage

```bash
cd s3-exporter
mvn clean install
java -jar target/s3-exportor-1.0-SNAPSHOT.jar  -input $PWD/src/main/resources/orders.json -output $PWD/src/main/resources/orderInventory2.parquet
```

- Note: Need to add more logic to upload parquet to s3.

- Not ready for usage still in development

- required input: 
	* -input - location of the json file that will be converted to parquet
	* -output - location of output parquet file.

# Demo

[![asciicast](https://asciinema.org/a/ao1irl3zbcuueddx9hyvi5f0l.png)](https://asciinema.org/a/ao1irl3zbcuueddx9hyvi5f0l)

