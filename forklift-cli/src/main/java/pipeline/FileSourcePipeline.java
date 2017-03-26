package pipeline;

import com.analyticsio.kafkaexporter.Converter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import model.Order;

/**
 * Created by zhassan on 2017-03-16.
 */
public class FileSourcePipeline implements SourcePipeline {

    public static final String DEFAULT_DATAFORMAT = "json";
    String source;
    String inputFormat;
    Map<String,String> properties;


    public String getSource() {
        return source;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public FileSourcePipeline(String source, Map<String,String> properties) {
        this.source=source;
        this.inputFormat=properties.getOrDefault("dataformat", DEFAULT_DATAFORMAT);
        this.properties= properties;
    }

    /**
     * TODO: make this class more generic.
     * @return
     */
    @Override
    public List<Order> getData() {

        // Read in Json from file and convert to List<Order> orders
        List<Order> data=null;
        try {
             data = Converter.getOrders(source);



        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    @Override
    public String toString() {
        return "FileSourcePipeline{" +
                "source='" + source + '\'' +
                ", inputFormat='" + inputFormat + '\'' +
                ", properties=" + properties +
                '}';
    }
}
