package com.analyticsio.kafkaexporter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import dataformats.utils.SchemaUtils;
import model.Order;

/**
 * Entrypoint for cli for setting up and configuring input
 *
 */
public class App 
{


    @Parameter(names = "-kafkaUrl", description = "Url to KafkaBroker")
    String kafkaUrl= "localhost:9092";

    @Parameter(names = "-topic", description = "Topic in Kafka to read from")
    String topic= "topicA";


    @Parameter(names = "-input", description = "Input file location. Default json",required = true)
    String input;
    
    @Parameter(names = "-output",
            description = "Output file location. Default Parquet",required = true)
    String output;


    public static void main( String[] args ){
        App main = new App();
        JCommander commander = new JCommander(main, args);
        main.run(commander);
    }


    public void run(JCommander commander){
        //AmazonS3 s3client = getAmazonS3Client();
        //listBuckets(s3client);
        extractJson();

        System.out.println( "Done!" );

    }

    private void extractJson() {

        // Read in Json from file and convert to List<Order> orders
        try {
            byte[] jsonData = Files.readAllBytes(Paths.get(input));
            ObjectMapper mapper = new ObjectMapper();
           // Order order= mapper.readValue(jsonData, Order.class);
            List<Order> myOrders = mapper.readValue(jsonData, mapper.getTypeFactory().constructCollectionType(List.class, Order.class));

            // get avro schema
            Schema schema= SchemaUtils.toAvroSchema(Order.class);

            List<GenericRecord> list= new ArrayList<>();
            for (Order order:myOrders  ) {
                GenericRecord item =createOrderRecord(order,schema);
                System.out.println("order: "+order);
                list.add(item);
            }

            writeParquetFile(list,schema, true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * Generic Record required before writing to parquet format.
     *
     * @param order
     * @param schema
     * @return
     */
    private GenericRecord createOrderRecord(Order order,Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("_id", order.getId());
        record.put("productName",order.getProductName());
        record.put("productPrice",order.getProductPrice());
        record.put("productCategory",order.getProductCategory());
        record.put("productQuantity",order.getProductQuantity());
        return record;
    }

    /**
     * Writes parquet to a file in local directory then uploads file to s3 destination
     * @param genericRecords
     * @param schema
     * @throws IOException
     */
    private void writeParquetFile(List<GenericRecord> genericRecords, Schema schema, Boolean overwrite) throws IOException {
         CompressionCodecName comp = CompressionCodecName.SNAPPY;
        int bSize = 256 * 1024 * 1024;
        int pSize = 64 * 1024;
        if(Files.exists(Paths.get(output)) && overwrite.booleanValue() == true){
            System.out.println("Overwriting file: "+ output);
            Files.delete(Paths.get(output));

        }
        try (AvroParquetWriter<GenericRecord> writer =  new AvroParquetWriter<GenericRecord>(new Path("file://" + output), schema, comp, bSize, pSize)) {
            for (GenericRecord record : genericRecords) {
                writer.write(record);
            }
            System.out.println("Done Writing Parquet file: "+ output);
            writer.close();
        }
    }


    private static void listBuckets(AmazonS3 s3client) {
        for (Bucket bucket : s3client.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }
     }

    private static AmazonS3 getAmazonS3Client() {
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        return new AmazonS3Client(credentials);
    }
}
