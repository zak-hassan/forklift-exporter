package com.analyticsio.model;

/**
 * Created by zhassan on 2017-04-04.
 */
public class Pipeline {


    String source;
    String sink;

    @java.lang.Override
    public java.lang.String toString() {
        return "Pipeline{" +
                "source='" + source + '\'' +
                ", sink='" + sink + '\'' +
                '}';
    }

    public Pipeline(){

    };


    public Pipeline(String source, String sink) {
        this.source = source;
        this.sink = sink;
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
}
