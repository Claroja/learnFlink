package com.claroja.model;

public class CEPLoginEvent {
    public String userId;
    public String ipAddr;
    public String eventType;
    public Long eventTime;

    public CEPLoginEvent(String userId, String ipAddr, String eventType, Long eventTime) {
        this.userId = userId;
        this.ipAddr = ipAddr;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public CEPLoginEvent() {
    }

    @Override
    public String toString() {
        return "CEPLoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddr='" + ipAddr + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}
