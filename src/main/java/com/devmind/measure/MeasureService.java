package com.devmind.measure;

import com.devmind.cassandra.poc.CustomChronoUnit;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class MeasureService {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MeasureService.class);

    public List<MeasureDTO> computeAverageOnTimeRange(List<Measure> values, CustomChronoUnit group, Instant start){
        LOG.info("data are grouped by {}", group);
        //We compute average by grouping on seconds, minutes....
        Map<LocalDateTime, Double> averages =
                values.stream().collect(Collectors.groupingBy(
                        e -> e.getEventTime().truncatedTo(group),
                        Collectors.averagingDouble(e -> e.getValue())));

        Instant step = Instant.now();
        LOG.info("data read first step in {} ms", Duration.between(start, step));
        List<MeasureDTO> dtos = averages
                .entrySet()
                .stream()
                .map(e -> new MeasureDTO().setValue(e.getValue().longValue()).setEventTime(e.getKey()))
                .collect(Collectors.toList());
        LOG.info("data read second step in {} ms", Duration.between(step, Instant.now()).toMillis());

        return dtos;
    }
}
