package com.analyticsio.kafkaexporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.StringUtils;
import pipeline.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
/**
 * Entrypoint for cli for setting up and configuring input
 *
 * forklift -source file://$PWD/src/main/resources/orders.json?dataformat=json
 * -sink s3://bucket/foldername/file?region=us-east-2&dataformat=parquet
 * -avroSchema file://$PWD/orderSchema.json
 *
 */
public class App 
{

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static final String S3_SUFFIX = "s3://";
    public static final String FILE_SUFFIX = "file://";
    public static final String  DEFAULT_INPUT="json";
    public static final String  DEFAULT_OUTPUT= "json";
    public static final String CASSANDRA_PREFIX = "cassandra://";
    public static final String HDFS_PREFIX = "hdfs://";

    @Parameter(names = "-source", description = "Source of the data being imported",required = false)
    String source="file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet"; //="file:///Users/zhassan/git/kafka-s3-exporter/forklift-cli/src/main/resources/orders.json?dataformat=json";
//="file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet";
    @Parameter(names = "-sink", description = "Target location to export data to" ,required = false)
    String sink= "hdfs://localhost:9000/test2/";
    //="cassandra://192.168.33.40:9042?keyspace=product&table=customerOrder";
    //String sink;//="s3://analyticsio-sandbox/demo1/example2.parquet?region=us-east-2&dataformat=parquet";

    @Parameter(names = "-avroSchema", description = "Source of the data being imported")
    String avroSchema;//="file://$PWD/orderSchema.json";



    public static void mergeParquetFiles(String dir, String resultFile){
        //TODO: Get a directory and then generate a single parquet file using:

    }


    public static void main( String[] args ){
        App main = new App();
        JCommander commander = new JCommander(main, args);
        main.run(commander);
    }


    public void run(JCommander commander){
        prepareSinkAndSource();
        LOGGER.info( "run (): Done!" );
        System.exit(0);
    }

    /**
     * Prepare sink and source and run data pipeline
     */
    private void prepareSinkAndSource() {

        try {
            SourcePipeline sourcePipe = getSourcePipeline();
            SinkPipeline sinkPipe = getSinkPipeline();
            DataPipeline pipeline= new DataPipeline(sourcePipe,sinkPipe);
            pipeline.execute();
        }catch(Exception ex){
            System.err.println(ex.getMessage());
        }finally{
            LOGGER.info("job complete. ");
        }
    }

    private String getFullPath(String originalSource, String fileSuffix) throws Exception {
        String[] fileLocation= originalSource.split(fileSuffix);
        if(fileLocation.length !=2 ) {
            throw new Exception("Error: Need to provide "+fileSuffix+" + Absolute path to file");
        }
        return fileLocation[1];
    }

    private Map<String,String> getProperties(String fileSuffix, String s) {
        Map<String, String> prop= new HashMap<String,String>();
        if(s.indexOf("?") > -1){
            String[] fileProps= s.split(("[?]"));
//            LOGGER.info("fileProps: "+fileProps[1]);
           if(fileSuffix.contentEquals(fileSuffix)) {
               String[] properties= fileProps[1].split("&");
               LOGGER.info(" Properties.length: "+properties.length+ " filename: "+s+ "filesuffix "+fileSuffix);
               for (String p: properties) {
                   LOGGER.info(" PROPERTIES: " + p);
                   String[] property = p.split("=");
                   prop.put(property[0],property[1]);
               }
           }
        }
        return prop;
    }



    private SourcePipeline getSourcePipeline() throws Exception {
        SourcePipeline sourcePipe=null;
        if(source.startsWith(FILE_SUFFIX)){
            String file = getFullPath(source, FILE_SUFFIX);
            Map<String,String> prop= getProperties(FILE_SUFFIX, file);
            LOGGER.info("Properties: "+ prop);
            String fileName= stripQueryString(file);
            sourcePipe= new FileSourcePipeline(fileName, prop);
//            LOGGER.info("fileLocation: "+ file);
            LOGGER.info("SourcePipe: "+ sourcePipe);

        } else if(source.startsWith(S3_SUFFIX)){
            String file = getFullPath(source, S3_SUFFIX);
            Map<String,String> prop= getProperties(S3_SUFFIX, file);
            String fileName= stripQueryString(file);

            sourcePipe= new S3SourcePipeline(fileName,  prop);
//            LOGGER.info("S3 fileLocation: "+ file);
            LOGGER.info(prop.toString());
            LOGGER.info("SourcePipe: "+ sourcePipe);

        }else {
            LOGGER.info("Unknown component");
            throw new Exception("Error: Source Unknown component used. Please use file:// or s3:// ");
            //TODO: Throw exception if an unsupported component is used.
        }
        return sourcePipe;
    }

    private String stripQueryString(String file){
        String[] fileName= file.split("[?]");
        if(fileName.length==2)
                return fileName[0];
        return file;
    }
    private SinkPipeline getSinkPipeline() throws Exception {
        SinkPipeline sinkPipe=null;
        if(sink.startsWith(FILE_SUFFIX)){
            String file = getFullPath(sink,FILE_SUFFIX);
            Map<String,String> prop= getProperties(FILE_SUFFIX, file);

            String fileName= stripQueryString(file);
            sinkPipe= new FileSinkPipeline(fileName, prop);
            LOGGER.info("SinkPipe: "+ sinkPipe);
        } else if(sink.startsWith(S3_SUFFIX)) {
            String file = getFullPath(sink, S3_SUFFIX);
            Map<String, String> prop = getProperties(S3_SUFFIX, file);
            LOGGER.info(prop.toString());
            String fileName = stripQueryString(file);
            sinkPipe = new S3SinkPipeline(fileName, prop);
            LOGGER.info("SinkPipe: " + sinkPipe);
        } else if(sink.startsWith(CASSANDRA_PREFIX)){
            String file = getFullPath(sink, CASSANDRA_PREFIX);
            String serverUrl= getCassandraServerUrl(file);
            int serverPort= getCassandraPort(file);
            sinkPipe= new CassandraSinkPipeline(serverUrl,serverPort,null);
            LOGGER.info("SinkPipe: " + sinkPipe);

        }else if(sink.startsWith(HDFS_PREFIX)){
            String file = getFullPath(sink, HDFS_PREFIX);
            String dest = getHDFSDest(file);
            String serverUrl= getHDFSServerUrl(file);
             sinkPipe= new HDFSSinkPipeline(serverUrl,dest,null);
            LOGGER.info("SinkPipe: " + sinkPipe);

        }   else {
            LOGGER.info("Sink: Unknown component");
            throw new Exception("Error: Sink Unknown component used. Please use file:// or s3:// ");

            //TODO: Throw exception if an unsupported component is used.
        }
        return sinkPipe;
    }

    private String getHDFSDest(String file) {
        String[] directories=file.split("/");

        if (directories.length ==2)
            return "/"+directories[1]+"/";


        String[] list = Arrays.copyOfRange(directories, 1, directories.length);
        return String.join("/", list);
   }

    private String getHDFSServerUrl(String file) {
        String[] urlBits = file.split("/");


        return urlBits[0];
    }

    private int getCassandraPort(String file) {
        return 9042;
    }

    private String getCassandraServerUrl(String file) {
        return "192.168.33.40";
    }

}
