package com.devmind.cassendra.poc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;

public class CassendraClient {

    private Cluster cluster;
    private Session session;
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CassendraClient.class);

    public CassendraClient connect(String node) {
        cluster = Cluster.builder().withPort(9042).addContactPoint(node).build();

        Metadata metadata = cluster.getMetadata();
        LOG.debug("Connected to cluster: {}", metadata.getClusterName());
        for (Host host : metadata.getAllHosts() ) {
            LOG.debug("Datacenter: {}; Host: {}; Rack: {}", host.getDatacenter(), host.getAddress(), host.getRack());
        }

        session = cluster.connect();
        return this;
    }

    public void close() {
        cluster.close();
    }

    public Session getSession() {
        return this.session;
    }
}
