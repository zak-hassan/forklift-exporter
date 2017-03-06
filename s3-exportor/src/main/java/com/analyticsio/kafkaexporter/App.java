package com.analyticsio.kafkaexporter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

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


    @Parameter(names = "-input", description = "DataFormat that we read Kafka data . Default json")
    String input= "json";

    @Parameter(names = "-output",
            description = "DataFormat that we write Kafka data. Default json."+
                    "Supports avro, parquet, json or thrift ")
    String output= "json";


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
        System.out.println("kafkaBrokerUrl: " + kafkaUrl);
        System.out.println("Topic: " + topic);

        //readMessages();

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
