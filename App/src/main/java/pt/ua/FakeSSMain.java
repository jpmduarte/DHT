package pt.ua;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class FakeSSMain {
    public static void main(String[] args) {
        ReactiveNodeClient client = new ReactiveNodeClient();
        NodeInfo bootstrap = new NodeInfo("node-1", "localhost", 7878);

        Instant fixed = Instant.parse("2026-06-09T12:00:00Z");
        String[] zones = {"eu", "us", "af", "asia", "latam", "me"};

        for (int i = 1; i <= 18; i++) {
            Map<String, String> fields = new HashMap<>();
            String zone = zones[i % zones.length];
            fields.put("zone", zone);
            fields.put("sensor", "s" + i);

            Event event = new Event("dev-" + i, "temperature", "zone-a", fields, fixed);
            System.out.println("[fake-ss] Sending event " + i + ": " + JsonEventCodec.toJson(event));

            client.ingest(bootstrap, event, "zone").block();
        }

        System.out.println("[fake-ss] Done.");
    }
}