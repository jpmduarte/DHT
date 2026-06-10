package pt.ua;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class FakeSSMain {
    public static void main(String[] args) {
        ReactiveNodeClient client = new ReactiveNodeClient();
        String host = args.length >= 1 ? args[0] : "localhost";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 7878;
        NodeInfo bootstrap = new NodeInfo("bootstrap", host, port);

        Instant fixed = Instant.parse("2026-06-09T12:00:00Z");
        String[] zones = {"eu", "us", "af", "asia", "latam", "me"};

        for (int i = 1; i <= 18; i++) {
            Map<String, String> fields = new HashMap<>();
            String zone = zones[i % zones.length];
            fields.put("zone", zone);
            fields.put("sensor", "s" + i);

            Event event = new Event("dev-" + i, "temperature", "zone-a", fields, fixed);
            System.out.println("[fake-ss] Sending event " + i + ": " + JsonEventCodec.toJson(event));

            try {
                client.ingest(bootstrap, event, "zone").blockingAwait();
            } catch (RuntimeException e) {
                System.err.println("[fake-ss] Ingest failed: " + e.getMessage());
                System.err.println("[fake-ss] Check that the bootstrap DHT node is running at "
                        + bootstrap.getHost() + ":" + bootstrap.getPort());
                throw e;
            }
        }

        System.out.println("[fake-ss] Done.");
    }
}
