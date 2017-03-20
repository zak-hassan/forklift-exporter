package com.analyticsio.kafkaexporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

import pipeline.DataPipeline;
import pipeline.FileSinkPipeline;
import pipeline.FileSourcePipeline;
import pipeline.S3SinkPipeline;
import pipeline.S3SourcePipeline;
import pipeline.SinkPipeline;
import pipeline.SourcePipeline;

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

    public static final String S3_SUFFIX = "s3://";
    public static final String FILE_SUFFIX = "file://";
    public static final String  DEFAULT_INPUT="json";
    public static final String  DEFAULT_OUTPUT= "json";

    @Parameter(names = "-source", description = "Source of the data being imported",required = true)
    String source;//="file:///Users/zhassan/git/kafka-s3-exporter/s3-exportor/src/main/resources/orders.json?dataformat=json";

    @Parameter(names = "-sink", description = "Target location to export data to", required = true)
    String sink;//="s3://analyticsio-sandbox/demo1/example2.parquet?region=us-east-2&dataformat=parquet";

    @Parameter(names = "-avroSchema", description = "Source of the data being imported")
    String avroSchema;//="file://$PWD/orderSchema.json";


    public static void main( String[] args ){
        App main = new App();
        JCommander commander = new JCommander(main, args);
        main.run(commander);
    }


    public void run(JCommander commander){
//        extractJson();


        prepareSinkAndSource();
        System.out.println( "run (): Done!" );
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
            System.out.println("prepareSinkAndSource() Done!");
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
//            System.out.println("fileProps: "+fileProps[1]);
           if(fileSuffix.contentEquals(fileSuffix)) {
               String[] properties= fileProps[1].split("&");
               System.out.println(" Properties.length: "+properties.length+ " filename: "+s+ "filesuffix "+fileSuffix);
               for (String p: properties) {
                   System.out.println(" PROPERTIES: " + p);
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
            System.out.println("Properties: "+ prop);
            String fileName= stripQueryString(file);
            sourcePipe= new FileSourcePipeline(fileName, prop);
//            System.out.println("fileLocation: "+ file);
            System.out.println("SourcePipe: "+ sourcePipe);

        } else if(source.startsWith(S3_SUFFIX)){
            String file = getFullPath(source, S3_SUFFIX);
            Map<String,String> prop= getProperties(S3_SUFFIX, file);
            String fileName= stripQueryString(file);

            sourcePipe= new S3SourcePipeline(fileName,  prop);
//            System.out.println("S3 fileLocation: "+ file);
            System.out.println(prop);
            System.out.println("SourcePipe: "+ sourcePipe);

        }else {
            System.out.println("Unknown component");
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
            System.out.println("SinkPipe: "+ sinkPipe);
        } else if(sink.startsWith(S3_SUFFIX)){
            String file = getFullPath(sink,S3_SUFFIX);
            Map<String,String> prop= getProperties(S3_SUFFIX, file);
            System.out.println(prop);
            String fileName= stripQueryString(file);
            sinkPipe= new S3SinkPipeline(fileName,  prop);
            System.out.println("SinkPipe: "+ sinkPipe);
        }else {
            System.out.println("Sink: Unknown component");
            throw new Exception("Error: Sink Unknown component used. Please use file:// or s3:// ");

            //TODO: Throw exception if an unsupported component is used.
        }
        return sinkPipe;
    }

}
