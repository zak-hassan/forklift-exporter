package pipeline;

import java.util.Map;

/**
 * Created by zhassan on 2017-03-16.
 */
public class FileSinkPipeline implements SinkPipeline {
    String sink;
    String outputFormat;
    Map<String,String> properties;


    public FileSinkPipeline(String sink, Map<String,String> properties) {
        this.sink=sink;
        this.outputFormat= properties.getOrDefault("dataformat","json");
        this.properties=properties;
    }

    @Override
    public void execute(SourcePipeline source) {
        source.getData();

    }

    @Override
    public String toString() {
        return "FileSinkPipeline{" +
                "sink='" + sink + '\'' +
                ", outputFormat='" + outputFormat + '\'' +
                ", properties=" + properties +
                '}';
    }
}
