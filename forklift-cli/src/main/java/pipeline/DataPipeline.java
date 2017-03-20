package pipeline;

import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Created by zhassan on 2017-03-16.
 */
public class DataPipeline {
    SourcePipeline source;
    SinkPipeline sink;

    public DataPipeline(SourcePipeline sourcePipe, SinkPipeline sinkPipe) {
        this.source=sourcePipe;
        this.sink=sinkPipe;
    }

    public void execute() throws JsonMappingException, ClassNotFoundException {
        sink.execute(source);

    }
}
