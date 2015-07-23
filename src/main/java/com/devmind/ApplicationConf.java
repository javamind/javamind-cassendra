package com.devmind;

import com.devmind.cassandra.poc.CassandraClient;
import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConf {

    public static final String CASSANDRA_NODE = "172.18.1.241";
    @Bean
    public CassandraClient cassendraClient(){
        return new CassandraClient(CASSANDRA_NODE);
    }

    @Bean
    public SparkConf sparkConf(){
        return new SparkConf()
                .setAppName("Poc Cassendra Spark")
                .setMaster("local[4]")
                .set("spark.cassandra.connection.host", CASSANDRA_NODE);
    }
}
