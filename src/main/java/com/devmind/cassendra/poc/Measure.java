package com.devmind.cassendra.poc;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Created by gehret.ext on 21/07/2015.
 */
public class Measure {
    private Long value;
    private LocalDateTime eventTime;

    public Long getValue() {
        return value;
    }

    public Measure setValue(Long value) {
        this.value = value;
        return this;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public Measure setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measure measure = (Measure) o;
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
                ", eventTime=" + DateTimeFormatter.ISO_DATE_TIME.format(eventTime) +
                '}';
    }
}
