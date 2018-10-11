package com.rsvps.model;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "rsvpsau")
public class MeetupRSVP {

    private Venue venue;
    private String visibility;
    private String response;
    private Integer guests;
    private Member member;
    private Integer rsvp_id;
    private Long mtime;
    private Event event;
    private Group group;

    public Venue getVenue() {
        return venue;
    }

    public void setVenue(Venue venue) {
        this.venue = venue;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public Integer getGuests() {
        return guests;
    }

    public void setGuests(Integer guests) {
        this.guests = guests;
    }

    public Member getMember() {
        return member;
    }

    public void setMember(Member member) {
        this.member = member;
    }

    public Integer getRsvp_id() {
        return rsvp_id;
    }

    public void setRsvp_id(Integer rsvp_id) {
        this.rsvp_id = rsvp_id;
    }

    public Long getMtime() {
        return mtime;
    }

    public void setMtime(Long mtime) {
        this.mtime = mtime;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

}
