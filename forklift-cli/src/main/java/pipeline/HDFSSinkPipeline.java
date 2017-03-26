package pipeline;

import com.analyticsio.kafkaexporter.HDFSHelper;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.util.Map;

/**
 * Created by zhassan on 26/03/17.
 */
public class HDFSSinkPipeline implements SinkPipeline {
    String serverUrl;
    String destinationPath;
    Map<String,String> properties;
    public HDFSSinkPipeline(String serverUrl,String dest, Map<String,String> properties) {
        this.serverUrl=serverUrl;
        this.destinationPath=dest;
        this.properties=properties;
    }

    @Override
    public void execute(SourcePipeline source) throws JsonMappingException, ClassNotFoundException {

        String src=source.getSource();
        HDFSHelper hdfs= new HDFSHelper(serverUrl);
        hdfs.uploadFileHDFS(src, destinationPath);
        System.out.println("Uploaded file to: src="+src+" dest="+destinationPath );
    }
}
