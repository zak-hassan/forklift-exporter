package pipeline;

import java.util.List;
import java.util.Map;

import model.Order;

/**
 * Created by zhassan on 2017-03-16.
 */
public class S3SourcePipeline implements SourcePipeline {
    String source;
    String inputFormat;
    Map<String,String> property;


    public S3SourcePipeline(String source,  Map<String,String> property) {
        this.source= source;
        this.inputFormat= property.getOrDefault("dataformat","json");
        this.property= property;
    }

    @Override
    public List<Order> getData() {

        return null;
    }

    @Override
    public String toString() {
        return "S3SourcePipeline{" +
                "source='" + source + '\'' +
                ", inputFormat='" + inputFormat + '\'' +
                ", property=" + property +
                '}';
    }
}
