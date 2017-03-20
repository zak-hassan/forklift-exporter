package pipeline;

import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Created by zhassan on 2017-03-16.
 */
public interface SinkPipeline {
    void execute(SourcePipeline source) throws JsonMappingException, ClassNotFoundException;
}
