package com.analyticsio.model;

/**
 * Created by zhassan on 2017-04-04.
 */
public class Pipeline {

    int id;
    String source;
    String sink;
    STATUSCONSTANT status;

    @Override
    public String toString() {
        return "Pipeline{" +
                "source='" + source + '\'' +
                ", sink='" + sink + '\'' +
                ", status=" + status +
                '}';
    }

    public Pipeline(){ }

    public Pipeline(String source, String sink) {
        this.source = source;
        this.sink = sink;
    }

    public void startPipeline(){
        this.status = STATUSCONSTANT.INPROGRESS;
    }
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    public STATUSCONSTANT getStatus() {
        return status;
    }

    public void setStatus(STATUSCONSTANT status) {
        this.status = status;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

}
