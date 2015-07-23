package com.devmind.measure;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

/**
 * Created by gehret.ext on 21/07/2015.
 */
public class MeasureDTO2 {
    private String id;
    private Long value;
    private Date eventTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getValue() {
        return value;
    }

    public MeasureDTO2 setValue(Long value) {
        this.value = value;
        return this;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public MeasureDTO2 setEventTime(Date eventTime) {
        this.eventTime = eventTime;
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeasureDTO2 measure = (MeasureDTO2) o;
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
