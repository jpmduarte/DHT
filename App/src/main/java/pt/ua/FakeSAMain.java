package pt.ua;

import java.time.LocalDate;

public class FakeSAMain {
    public static void main(String[] args) {
        ReactiveNodeClient client = new ReactiveNodeClient();
        NodeInfo bootstrap = new NodeInfo("node-1", "localhost", 7878);
        LocalDate day = LocalDate.of(2026, 6, 9);

        query(client, bootstrap, day, "zone", "eu");
        query(client, bootstrap, day, "zone", "us");
        query(client, bootstrap, day, "zone", "asia");
        query(client, bootstrap, day, "zone", "latam");

    }

    private static void query(ReactiveNodeClient client, NodeInfo bootstrap, LocalDate day, String indexField, String indexValue) {
        System.out.println("[fake-sa] Requesting stream for day=" + day + ", field=" + indexField + ", value=" + indexValue);

        client.streamSubSeries(bootstrap, day, indexField, indexValue)
                .doOnNext(ev -> System.out.println("[fake-sa] Event received: " + JsonEventCodec.toJson(ev)))
                .doOnComplete(() -> System.out.println("[fake-sa] Stream complete for value=" + indexValue))
                .blockLast();
    }
}