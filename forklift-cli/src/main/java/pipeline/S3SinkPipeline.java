package pipeline;

import com.analyticsio.kafkaexporter.Converter;

import org.apache.avro.Schema; //this is used to serialize items in json format
import org.apache.avro.generic.GenericRecord; //

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map; //maps key items to values

import dataformats.utils.SchemaUtils; 
import model.Order;

/**
 * Created by zhassan on 2017-03-16.
 */
public class S3SinkPipeline implements SinkPipeline {

    String sink;
    String outputFormat;
    Map<String,String> properties;


    public S3SinkPipeline(String sink, Map<String,String> properties) {
        this.sink= sink;
        this.outputFormat= properties.getOrDefault("dataformat","json");
        this.properties= properties;
    }

    @Override
    public void execute(SourcePipeline source)  {
        List<Order> myOrders= source.getData();
        // get avro schema
        try {

        //
        Schema schema = SchemaUtils.toAvroSchema(Order.class);
        // Serialize orders to avro records
//            System.out.println(schema.toString(true));
        List<GenericRecord> list = Converter.getGenericRecords(myOrders, schema);
//
//        // convert avro serialized record to parquet

            String bucketname=sink.split("/")[0];
            List<String> s3dir=Arrays.asList(sink.split("/")).subList(1,sink.split("/").length);
           String s3fileName= String.join("/", s3dir);
            String output=System.getenv("HOME")+"/.forklift-exporter/"+bucketname +"/"+ s3fileName;

            System.out.println("S3filename: " + s3fileName);
            System.out.println("Sink Split: " + sink.split("/")[0]);
            System.out.println("Sink Output: " + output);

            Converter.writeParquetFile(bucketname,s3fileName,properties.get("region"), output, list, schema, true);

            //Converter.writeParquetFile(bucketname,sink, "/tmp/orderInventory2.parquet", list, schema, true, "analyticsio-sandbox");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "S3SinkPipeline{" +
                "sink='" + sink + '\'' +
                ", outputFormat='" + outputFormat + '\'' +
                ", property=" + properties +
                '}';
    }
}
