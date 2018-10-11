package com.rsvps.model;

import java.util.List;

public class Group {
    
    private List<GroupTopic> group_topics = null;    
    private String group_city;    
    private String group_country;    
    private Integer group_id;    
    private String group_name;    
    private Double group_lon;    
    private String group_urlname;    
    private String group_state;    
    private Double group_lat;

    public List<GroupTopic> getGroup_topics() {
        return group_topics;
    }

    public void setGroup_topics(List<GroupTopic> group_topics) {
        this.group_topics = group_topics;
    }

    public String getGroup_city() {
        return group_city;
    }

    public void setGroup_city(String group_city) {
        this.group_city = group_city;
    }

    public String getGroup_country() {
        return group_country;
    }

    public void setGroup_country(String group_country) {
        this.group_country = group_country;
    }

    public Integer getGroup_id() {
        return group_id;
    }

    public void setGroup_id(Integer group_id) {
        this.group_id = group_id;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public Double getGroup_lon() {
        return group_lon;
    }

    public void setGroup_lon(Double group_lon) {
        this.group_lon = group_lon;
    }

    public String getGroup_urlname() {
        return group_urlname;
    }

    public void setGroup_urlname(String group_urlname) {
        this.group_urlname = group_urlname;
    }

    public String getGroup_state() {
        return group_state;
    }

    public void setGroup_state(String group_state) {
        this.group_state = group_state;
    }

    public Double getGroup_lat() {
        return group_lat;
    }

    public void setGroup_lat(Double group_lat) {
        this.group_lat = group_lat;
    }
    
    
}