package com.analyticsio.model;

/**
 * Created by zhassan on 2017-04-04.
 */
public class PipelineStatus {


    public PipelineStatus(Pipeline pipeline, String msg) {
        this.pipeline = pipeline;
        this.msg = msg;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    Pipeline pipeline;
    String msg;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
