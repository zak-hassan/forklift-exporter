package pipeline;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.datastax.driver.core.Cluster;

import com.datastax.driver.core.Host;

import com.datastax.driver.core.Metadata;

import com.datastax.driver.core.Session;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import model.Order;

/**
 * Created by zhassan on 2017-03-21.
 */
public class CassandraSinkPipeline implements SinkPipeline {

    String serverUrl;
    int port;
     Map<String,String> properties;
    private Cluster cluster;
    private Session session;

    public CassandraSinkPipeline(String serverUrl, int port,  Map<String, String> properties) {
        this.serverUrl = serverUrl;
        this.port = port;
        this.properties = properties;
    }

    @Override
    public void execute(SourcePipeline source) throws JsonMappingException, ClassNotFoundException {

        dbconnect(serverUrl, port);
        List<Order> myOrders= source.getData();

        for (Order order: myOrders) {
            System.out.println("order"+order);
            getSession().execute("INSERT into product.customerOrder ( id, productName, productPrice, productCategory, productQuantity) VALUES (?, ?, ?, ?, ?)",
                    UUID.randomUUID(), order.getProductName(), order.getProductPrice(), order.getProductCategory(),order.getProductQuantity());

        }

        cluster.close();


    }

    private void dbconnect(String serverUrl, int port) {
        this.cluster = Cluster.builder().addContactPoint("192.168.33.40").withPort(9042).build();

        final Metadata metadata = cluster.getMetadata();

        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (final Host host : metadata.getAllHosts())

        {

            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",

                    host.getDatacenter(), host.getAddress(), host.getRack());

        }

        session = cluster.connect();
     }

    public Session getSession() {
        return session;
    }

    @Override
    public String toString() {
        return "CassandraSinkPipeline{" +
                "serverUrl='" + serverUrl + '\'' +
                ", port=" + port +
                ", properties=" + properties +
                '}';
    }
}
