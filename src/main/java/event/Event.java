package event;

import java.io.Serializable;
import java.util.UUID;

public class Event implements Serializable {
    private static final long serialVersionUID = 1;

    public String id;
    public String key;
    public String data;
    private long taskindex;
    private long sequenceNumber;
    private long timestamp;

    public Event() {
    }

    public Event(String key, String data, long timestamp, long sequenceNumber, long taskindex) {
        this.id = UUID.randomUUID().toString();
        this.key = key;
        this.data = data;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.taskindex = taskindex;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTaskIndex() {
        return taskindex;
    }

    public void setTaskIndex(long index) {
        this.taskindex = index;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString() {
        return "GeneratedEvent{" +
                "id='" + id + '\'' +
                ", key='" + key + '\'' +
                ", taskindex='" + String.valueOf(taskindex) + '\'' +
                ", sequence='" + String.valueOf(sequenceNumber) + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
