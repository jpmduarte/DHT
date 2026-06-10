package pt.ua;

import java.time.Instant;
import java.util.Map;

public class Event {
    private String deviceId;
    private String type;
    private String zone;
    private Map<String, String> fields;
    private Instant timestamp;

    public Event() {
    }

    public Event(String deviceId, String type, String zone, Map<String, String> fields, Instant timestamp) {
        this.deviceId = deviceId;
        this.type = type;
        this.zone = zone;
        this.fields = fields;
        this.timestamp = timestamp;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getField(String key) {
        if (fields != null && fields.containsKey(key)) {
            return fields.get(key);
        }

        switch (key) {
            case "deviceId":
                return deviceId;
            case "type":
                return type;
            case "zone":
                return zone;
            case "timestamp":
                return timestamp == null ? null : timestamp.toString();
            default:
                return null;
        }
    }
}
