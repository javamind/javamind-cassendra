package com.devmind.spark.poc;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.SomeColumns;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.devmind.cassandra.poc.CassandraClient;
import com.devmind.measure.Measure;
import com.devmind.measure.MeasureDTO;
import com.devmind.measure.MeasureDTO2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.collection.Seq;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.column;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

@RestController
@RequestMapping(value = "/spark")
public class SparkController {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CassandraClient.class);

    @Autowired
    private SparkConf sparkConf;

    @RequestMapping(value = "/get/{id}")
    public ResponseEntity<List<MeasureDTO2>> insert(@PathVariable(value = "id") String timeserie,
                     @RequestParam(value = "year", defaultValue = "2015") int year,
                     @RequestParam(value = "month", defaultValue = "7") int month,
                     @RequestParam(value = "date", defaultValue = "23") int date){

        Instant start = Instant.now();
        try(JavaSparkContext sc = new JavaSparkContext(sparkConf)) {


            LOG.info("generate Data");
            CassandraConnector connector = CassandraConnector.apply(sc.getConf());

            //CassandraJavaUtil.
//        try(Session session = connector.openSession()){
//            session.execute("DROP KEYSPACE IF EXISTS java_api");
//            session.execute("CREATE KEYSPACE java_api  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
//            session.execute("CREATE TABLE java_api.measure( \n" +
//                    "measurement_id text, \n" +
//                    "event_time timestamp, \n" +
//                    "value bigint, \n" +
//                    "PRIMARY KEY (measurement_id, event_time) \n" +
//                    ");");
//        }

            JavaRDD<MeasureDTO2> cassandraRowsRDD =
                    javaFunctions(sc)
                            .cassandraTable("ep_measurement", "measurement_timeseries1", mapRowTo(MeasureDTO2.class))
                            .select(
                                    column("event_time").as("name"),
                                    column("value").as("birthDate"));


            List<MeasureDTO2> dtos = cassandraRowsRDD.toArray();

            LOG.info("data read in {} ms", Duration.between(start, Instant.now()).toMillis());

            return new ResponseEntity<>(dtos, HttpStatus.OK);

//        LOG.info("Add random data");
//        LocalDateTime dateTime = LocalDateTime.of(year, month, date, 0, 0, 0);
//        List<Measure> measures = new ArrayList<>();
//
//        while (dateTime.isBefore(LocalDateTime.now())) {
//            dateTime = dateTime.plusSeconds(15);
//            measures.add(new Measure().setEventTime(dateTime).setValue((long) (Math.random() * 100000)));
//        }
//        JavaRDD<Measure> measuresRDD = sc.parallelize(measures);
//        CassandraJavaUtil.javaFunctions(measuresRDD).writerBuilder("measurement_id", "event_time", "value", mapToRow(Person.class)).saveToCassandra();
//
//        CassandraJavaUtil.javaFunctions(measuresRDD).saveToCassandra("java_api", "measure", SomeColumns.seqToSomeColumns(Seq."word", "count"));
//        LOG.info("show result");

        }

    }

}
