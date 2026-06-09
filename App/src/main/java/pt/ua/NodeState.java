package pt.ua;

import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeState {
    private final String nodeId;
    private final Map<SeriesKey, SeriesPartition> partitions = new ConcurrentHashMap<>();
    private final DiskSeriesStore diskStore;

    public NodeState(String nodeId, DiskSeriesStore diskStore) {
        this.nodeId = nodeId;
        this.diskStore = diskStore;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void ingest(Event event, String indexField) throws Exception {
        String indexValue = event.getField(indexField);
        if (indexValue == null) {
            System.out.println("[state] " + nodeId + " skipping event without " + indexField);
            return;
        }

        LocalDate day = event.getTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate();
        SeriesKey key = new SeriesKey(day, indexField, indexValue);

        System.out.println("[state] " + nodeId + " ingest key=" + key);
        partitions.computeIfAbsent(key, SeriesPartition::new).add(event);
        diskStore.append(event, key);
        System.out.println("[state] " + nodeId + " append done");
    }

    public Flux<Event> streamSubSeries(LocalDate day, String indexField, String indexValue) {
        SeriesKey key = new SeriesKey(day, indexField, indexValue);

        System.out.println("[state] " + nodeId + " stream key=" + key);

        SeriesPartition p = partitions.get(key);
        if (p != null) {
            System.out.println("[state] " + nodeId + " serving from memory");
            return Flux.fromIterable(p.snapshot());
        }

        System.out.println("[state] " + nodeId + " serving from disk");
        return diskStore.streamAll(key);
    }
}