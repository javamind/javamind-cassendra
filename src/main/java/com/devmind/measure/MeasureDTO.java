package com.devmind.measure;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Created by gehret.ext on 21/07/2015.
 */
public class MeasureDTO {
    private Long value;
    private String eventTime;

    public Long getValue() {
        return value;
    }

    public MeasureDTO setValue(Long value) {
        this.value = value;
        return this;
    }

    public String getEventTime() {
        return eventTime;
    }

    public MeasureDTO setEventTime(String eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    public MeasureDTO setEventTime(LocalDateTime eventTime) {
        this.eventTime = DateTimeFormatter.ISO_DATE_TIME.format(eventTime);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeasureDTO measure = (MeasureDTO) o;
        return Objects.equals(eventTime, measure.eventTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime);
    }

    @Override
    public String toString() {
        return "Measure{" +
                "value=" + value +
                ", eventTime=" + eventTime+
                '}';
    }
}
