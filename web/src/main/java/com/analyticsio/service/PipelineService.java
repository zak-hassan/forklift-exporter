package com.analyticsio.service;

import com.analyticsio.model.Pipeline;
import com.analyticsio.model.PipelineStatus;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhassan on 2017-04-04.
 */
public class PipelineService {

    public List<Pipeline> getAll() {
        return Arrays.asList(new Pipeline("file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet", "hdfs://localhost:9000/test5/"));
    }

    public PipelineStatus savePipeline(Pipeline pipeline){
        //TODO: Implement persistance here
        return new PipelineStatus(pipeline,"saved");
    }

    public PipelineStatus deletePipeline(String pipeId){
        //TODO: Implement persistance here
        return new PipelineStatus(new Pipeline("file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet", "hdfs://localhost:9000/test5/"),"deleted");
    }

    public Pipeline getOnePipeline(){
        return new Pipeline("file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet", "hdfs://localhost:9000/test5/");
    }

}
