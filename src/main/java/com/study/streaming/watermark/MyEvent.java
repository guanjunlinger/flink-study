package com.study.streaming.watermark;

import java.util.Date;

public class MyEvent {
    private String name;
    private Integer age;

    private Boolean isSpecial;

    private Long timestamp;

    public MyEvent(String name, Integer age, Boolean isSpecial) {
        this.name = name;
        this.age = age;
        this.isSpecial = isSpecial;
        this.timestamp = new Date().getTime();
    }

    public String getName() {
        return name;
    }

    public Integer getAge() {
        return age;
    }

    public Boolean getSpecial() {
        return isSpecial;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", isSpecial=" + isSpecial +
                ", timestamp=" + timestamp +
                '}';
    }
}
