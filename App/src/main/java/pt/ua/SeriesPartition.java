package pt.ua;

import java.util.ArrayList;
import java.util.List;

public class SeriesPartition {
    private final SeriesKey key;
    private final List<Event> events = new ArrayList<>();

    public SeriesPartition(SeriesKey key) {
        this.key = key;
    }

    public SeriesKey getKey() {
        return key;
    }

    public synchronized void add(Event event) {
        events.add(event);
    }

    public synchronized List<Event> snapshot() {
        return List.copyOf(events);
    }

    public synchronized int size() {
        return events.size();
    }
}