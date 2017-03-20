package pipeline;

import java.util.List;

import model.Order;

/**
 * Created by zhassan on 2017-03-16.
 */
public interface SourcePipeline {
    List<Order> getData();
}
