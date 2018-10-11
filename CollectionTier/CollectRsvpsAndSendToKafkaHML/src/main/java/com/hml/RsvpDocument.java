package com.hml;

import java.io.Serializable;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "temprsvps")
public class RsvpDocument implements Serializable {

    private static final long serialVersionUID = -33423L;
    
    private long id;
    private String data;
    private RsvpStatus status;   

    public RsvpDocument(Long id, String data, RsvpStatus status) {
        this.id = id;
        this.data = data;
        this.status = status;
    }

    public RsvpStatus getStatus() {
        return status;
    }

    public void setStatus(RsvpStatus status) {
        this.status = status;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
