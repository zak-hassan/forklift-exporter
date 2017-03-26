package com.analyticsio.kafkaexporter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import dataformats.utils.SchemaUtils;
import model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhassan on 2017-03-17.
 */
public class Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

    public static void convert(String input, String output, String s3filename, String bucketname) {
        // Read in Json from file and convert to List<Order> orders

        try {
            List<Order> myOrders = Converter.getOrders(input);

            // get avro schema
            Schema schema= SchemaUtils.toAvroSchema(Order.class);

            // serialize orders to avro records
            List<GenericRecord> list = getGenericRecords(myOrders, schema);

            // convert avro serialized record to parquet
           // writeParquetFile(bucketname,s3filename, Regions.fromName(),output, list, schema, true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static List<GenericRecord> getGenericRecords(List<Order> myOrders, Schema schema) {
        List<GenericRecord> list= new ArrayList<>();
        for (Order order:myOrders  ) {
            GenericRecord item =Converter.createOrderRecord(order,schema);
            list.add(item);
        }
        return list;
    }


    /**
     * TODO: Make this more generic
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static List<Order> getOrders(String in) throws IOException {
        byte[] jsonData= Files.readAllBytes(Paths.get(in));
        ObjectMapper mapper = new ObjectMapper();
        // Order order= mapper.readValue(jsonData, Order.class);
        return mapper.readValue(jsonData, mapper.getTypeFactory().constructCollectionType(List.class, Order.class));
    }

    /**
     * Generic Record required before writing to parquet format.
     *
     * @param order
     * @param schema
     * @return
     */
    public static GenericRecord createOrderRecord(Order order, Schema schema) {
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
     * @param bucketName
     * @param genericRecords
     * @param schema
     * @throws IOException
     */
    public static void writeParquetFile( String bucketName, String s3filename,String region, String output, List<GenericRecord> genericRecords,
                                        Schema schema, Boolean overwrite) throws IOException {
        CompressionCodecName comp = CompressionCodecName.SNAPPY;
        int bSize = 256 * 1024 * 1024;
        int pSize = 64 * 1024;
        if(Files.exists(Paths.get(output)) && overwrite.booleanValue() == true){
            LOGGER.info("Overwriting file: "+ output);
            Files.delete(Paths.get(output));

        }
        try (AvroParquetWriter<GenericRecord> writer =  new AvroParquetWriter<GenericRecord>(new Path("file://" + output), schema, comp, bSize, pSize)) {
            for (GenericRecord record : genericRecords) {
                writer.write(record);
            }
            LOGGER.info("Done Writing Parquet file: "+ output);
            writer.close();
        }
        LOGGER.info("Now uploading to s3!");
        AmazonS3 client= getAmazonS3Client(region);
        uploadFile(s3filename, bucketName, output, client);
    }


    public static void listBuckets(AmazonS3 s3client) {
        for (Bucket bucket : s3client.listBuckets()) {
            LOGGER.info(" - " + bucket.getName());
        }
    }

    public static AmazonS3 getAmazonS3Client() {
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        // Need to make this region selected via parameters rather then hard coded.
        return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_2).build();
    }
    public static AmazonS3 getAmazonS3Client(String region) {
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        // Need to make this region selected via parameters rather then hard coded.
        return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region).build();
    }

    public static void uploadFile(String fileName, String bucketName, String output_file , AmazonS3 client){
        client.putObject(new PutObjectRequest(bucketName, fileName,
                new File(output_file)));
        LOGGER.info("Done uploading");
    }

}
