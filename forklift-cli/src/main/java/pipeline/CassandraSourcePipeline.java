package pipeline;

import java.util.List;

import model.Order;

/**
 * Created by zhassan on 2017-03-21.
 */
public class CassandraSourcePipeline implements SourcePipeline {
    String source;

    @Override
    public String getSource() {
        return this.source;
    }

    @Override
    public List<Order> getData() {
        return null;
    }
}
