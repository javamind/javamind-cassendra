package com.devmind.cassendra.poc;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.ValueRange;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


public class CassendraControllerTest {
    List<Measure> measures = new ArrayList<>();

    {
        LocalDateTime now = LocalDateTime.now();
        for(int i=0; i<1000; i++){
            now = now.plusSeconds(15);
            measures.add(new Measure().setEventTime(now).setValue((long) (Math.random() * 100000)));
        }
    }

    @Test
    public void test(){

        LongSummaryStatistics values = measures.stream().mapToLong(Measure::getValue).summaryStatistics();
        //Temps par minute
//        Map<Measure, Long> test = measures.stream().map(e -> {
//            e.getEventTime().range(ChronoField.MINUTE_OF_DAY)
//            cal.setTime(e.getEventTime());
//        })

        Map<LocalDateTime, Double> repart =
                measures.stream().collect(Collectors.groupingBy(
                    e -> e.getEventTime().truncatedTo(ChronoUnit.HOURS),
                    Collectors.averagingDouble(e -> e.getValue())));

        repart.keySet().stream().peek(e -> System.out.println(e)).collect(Collectors.toList());

    }
}