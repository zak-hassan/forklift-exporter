package com.analyticsio.service;

import com.analyticsio.model.Pipeline;
import com.analyticsio.model.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhassan on 2017-04-04.
 */
public class PipelineService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineService.class);

    public List<Pipeline> getAll() {
        LOGGER.info("getAll Pipeline");
        return Arrays.asList(new Pipeline("file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet", "hdfs://localhost:9000/test5/"));
    }

    public PipelineStatus savePipeline(Pipeline pipeline){
        LOGGER.info("save Pipeline: " + pipeline);
        //TODO: Implement persistance here
        return new PipelineStatus(  "saved");
    }

    public PipelineStatus deletePipeline(String pipeId){
        //TODO: Implement persistance here
        LOGGER.info("pipeId: " + pipeId );
        return new PipelineStatus( "deleted");
    }

    public Pipeline getOnePipeline(String pipeId){
        LOGGER.info("getOne Pipeline: "+pipeId);

        return new Pipeline("file:///home/zhassan/Downloads/rocknroll.parquet?dataformat=parquet", "hdfs://localhost:9000/test5/");
    }

}
