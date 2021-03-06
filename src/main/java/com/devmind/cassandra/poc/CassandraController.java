package com.devmind.cassandra.poc;

import com.datastax.driver.core.*;
import com.devmind.measure.Measure;
import com.devmind.measure.MeasureDTO;
import com.devmind.measure.MeasureService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/cassendra")
public class CassandraController {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CassandraClient.class);

    @Autowired
    private CassandraClient client;

    @Autowired
    private MeasureService measureService;

    /**
     *
     * @param timeserie
     * @param group {@link ChronoUnit#MONTHS} {@link ChronoUnit#DAYS} {@link ChronoUnit#HOURS} {@link ChronoUnit#MINUTES} ...
     * @return
     */
    @RequestMapping(value = "/get/{id}")
    public ResponseEntity<List<MeasureDTO>> scan(@PathVariable(value = "id") String timeserie,
                                               @RequestParam(value = "group", required = false) CustomChronoUnit group) {
        Instant start = Instant.now();

        try {
            Session session = client.connect().getSession();
            LOG.info("Load all te data for measurement = [{}]", timeserie);
            ResultSet results = session.execute(String.format("SELECT * FROM ep_measurement.measurement_%s where measurement_id = '%s';", timeserie,timeserie));

            //Put all the values in a list
            List<Measure> values = new ArrayList<>();
            List<MeasureDTO> dtos;

            if(group!=null){
                for (Row row : results) {
                    values.add(
                            new Measure()
                                    .setValue(row.getLong("value"))
                                    .setEventTime(LocalDateTime.ofInstant(row.getDate("event_time").toInstant(), ZoneId.systemDefault())));
                }
                dtos = measureService.computeAverageOnTimeRange(values, group, start);
            }
            else{
                dtos = new ArrayList<>();
                for (Row row : results) {
                    dtos.add(
                            new MeasureDTO()
                                    .setValue(row.getLong("value"))
                                    .setEventTime(LocalDateTime.ofInstant(row.getDate("event_time").toInstant(), ZoneId.systemDefault())));
                }
            }

            LOG.info("data read in {} ms", Duration.between(start, Instant.now()).toMillis());
            return new ResponseEntity<>(dtos, HttpStatus.ACCEPTED);
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        } finally {
            client.close();
        }

    }


    @RequestMapping(value = "/insert1/{id}")
    public void insertOneData(@PathVariable(value = "id") String timeserie) {
         try {
            Session session = client.connect().getSession();
            PreparedStatement statement = session.prepare("INSERT INTO ep_measurement.measurement(measurement_id ,event_time,value ) VALUES (?,?,?);");

            BoundStatement boundStatement = new BoundStatement(statement);
            session.execute(boundStatement.bind(timeserie, new Date(), (long) (Math.random() * 100000)));
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            client.close();
        }

    }

    //on cherche à générer un jeu de données sur un mois ou on écrit une valeur par secondes
    // create a calendar
    @RequestMapping(value = "/insert/{id}")
    public void insert(
            @PathVariable(value = "id") String timeserie,
            @RequestParam(value = "year", defaultValue = "2015") int year,
            @RequestParam(value = "month", defaultValue = "7") int month,
            @RequestParam(value = "date", defaultValue = "20") int date) {

        Instant start = Instant.now();
        LocalDateTime dateTime = LocalDateTime.of(year, month, date, 0, 0, 0);
        try {
            Session session = client.connect().getSession();
            session.execute(String.format("CREATE TABLE IF NOT EXISTS ep_measurement.measurement_%s(measurement_id text,event_time timestamp,value bigint,PRIMARY KEY (measurement_id, event_time));", timeserie));
            PreparedStatement statement = session.prepare(String.format("INSERT INTO ep_measurement.measurement_%s(measurement_id ,event_time,value ) VALUES (?,?,?);", timeserie));

            BoundStatement boundStatement = new BoundStatement(statement);
            LOG.info("insert a data each 15seconds for timeserie = [{}] and year={}, month={}, date={} ==> {}", timeserie, year, month, date);
            while (dateTime.isBefore(LocalDateTime.now())) {
                dateTime = dateTime.plusSeconds(15);
                session.executeAsync(boundStatement.bind(timeserie, Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant()), (long) (Math.random() * 100000)));
            }
            LOG.info("data inserted in {} ms", Duration.between(start, Instant.now()).toMillis());
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            client.close();
        }

    }

    @RequestMapping(value = "/insertn/{id}")
    public void createData(@PathVariable(value = "id") String timeserie) throws InterruptedException {
        int cpt=500;
        for(int i=0 ; i<500 ; i++){
            LOG.info("{} remainings series ", cpt--);
            insert(timeserie+i, 2014, 7,20);
            if(i%5==0){
                Thread.sleep(14400);
            }
        }
        LOG.info("Batch is done");
    }
}
