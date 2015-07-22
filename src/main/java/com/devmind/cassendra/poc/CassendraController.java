package com.devmind.cassendra.poc;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

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
@EnableWebMvc
@RequestMapping(value = "/cassendra")
public class CassendraController {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CassendraClient.class);

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
        CassendraClient client = new CassendraClient();

        try {
            client.connect("127.0.0.1");
            Session session = client.getSession();
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

                LOG.info("data are grouped by {}", group);
                //We compute average by grouping on seconds, minutes....
                Map<LocalDateTime, Double> averages =
                        values.stream().collect(Collectors.groupingBy(
                                e -> e.getEventTime().truncatedTo(group),
                                Collectors.averagingDouble(e -> e.getValue())));

                Instant step = Instant.now();
                LOG.info("data read first step in {} ms", Duration.between(start, step));
                dtos = averages
                                .entrySet()
                                .stream()
                                .map(e -> new MeasureDTO().setValue(e.getValue().longValue()).setEventTime(e.getKey()))
                                .collect(Collectors.toList());
                LOG.info("data read second step in {} ms", Duration.between(step, Instant.now()).toMillis());
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


    @RequestMapping(value = "/load/{id}")
    public void loadData(@PathVariable(value = "id") String timeserie) {
        CassendraClient client = new CassendraClient();
        try {
            client.connect("127.0.0.1");
            Session session = client.getSession();
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
    public void loadAllData(
            @PathVariable(value = "id") String timeserie,
            @RequestParam(value = "year", defaultValue = "2015") int year,
            @RequestParam(value = "month", defaultValue = "7") int month,
            @RequestParam(value = "date", defaultValue = "20") int date) {

        Instant start = Instant.now();
        LocalDateTime dateTime = LocalDateTime.of(year, month, date, 0, 0, 0);
        CassendraClient client = new CassendraClient();
        try {
            client.connect("127.0.0.1");
            Session session = client.getSession();
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

    @RequestMapping(value = "/insert50/{id}")
    public void createData(@PathVariable(value = "id") String timeserie){
        for(int i=0 ; i<50 ; i++){
            loadAllData(timeserie+i, 2014, 7,20);
        }

    }
}
