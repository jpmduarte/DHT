package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Flowable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DhtNodeServer {
    private final NodeState state;
    private final NodeInfo self;
    private final PeerTable peers;
    private final ReactiveNodeClient client;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final ExecutorService connectionPool = Executors.newCachedThreadPool();

    public DhtNodeServer(NodeState state, NodeInfo self, PeerTable peers) {
        this.state = state;
        this.self = self;
        this.peers = peers;
        this.client = new ReactiveNodeClient();
    }

    public void start() {
        System.out.println("[server] Starting DHT node=" + self.getNodeId() + " on port=" + self.getPort());

        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(new InetSocketAddress(self.getPort()));
            while (true) {
                SocketChannel socket = server.accept();
                connectionPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            throw new RuntimeException("DHT node failed", e);
        }
    }

    private void handleConnection(SocketChannel socket) {
        String requestId = UUID.randomUUID().toString();
        try (SocketChannel channel = socket;
             BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8))) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    return;
                }

                DhtProtocol.Request request = mapper.readValue(line, DhtProtocol.Request.class);
                if (DhtProtocol.INGEST.equals(request.op)) {
                    handleIngest(request);
                    writeResponse(writer, DhtProtocol.Response.ack(requestId));
                } else if (DhtProtocol.QUERY.equals(request.op)) {
                    handleQuery(request, requestId, writer);
                } else {
                    writeResponse(writer, DhtProtocol.Response.error(requestId, "Unknown operation: " + request.op));
                }
            } catch (Exception e) {
                System.err.println("[server] request failed: " + e.getMessage());
                writeResponse(writer, DhtProtocol.Response.error(requestId, e.getMessage()));
            }
        } catch (Exception e) {
            System.err.println("[server] request failed: " + e.getMessage());
        }
    }

    private void handleIngest(DhtProtocol.Request request) {
        Event event = Objects.requireNonNull(request.event, "Missing event");
        String indexField = requireText(request.indexField, "Missing indexField");
        Objects.requireNonNull(event.getTimestamp(), "Missing event timestamp");
        state.validateIndexField(indexField);

        String indexValue = event.getField(indexField);
        if (indexValue == null) {
            return;
        }

        NodeInfo owner = ownerFor(eventDay(event), indexField, indexValue);
        if (owner.getNodeId().equals(self.getNodeId())) {
            try {
                state.ingest(event, indexField);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }

        client.ingest(owner, event, indexField).blockingAwait();
    }

    private void handleQuery(DhtProtocol.Request request, String requestId, BufferedWriter writer) {
        LocalDate day = LocalDate.parse(requireText(request.day, "Missing day"));
        String indexField = requireText(request.indexField, "Missing indexField");
        String indexValue = requireText(request.indexValue, "Missing indexValue");
        state.validateIndexField(indexField);

        NodeInfo owner = ownerFor(day, indexField, indexValue);
        Flowable<Event> source = owner.getNodeId().equals(self.getNodeId())
                ? state.streamSubSeries(day, indexField, indexValue)
                : client.streamSubSeries(owner, day, indexField, indexValue);

        source.blockingForEach(event -> writeResponse(writer, DhtProtocol.Response.event(requestId, event)));
        writeResponse(writer, DhtProtocol.Response.complete(requestId));
    }

    private NodeInfo ownerFor(LocalDate day, String indexField, String indexValue) {
        String shardKey = day + "|" + indexField + "|" + indexValue;
        BigInteger targetId = KeyHasher.hashToId(shardKey);
        return peers.responsibleNode(targetId, self);
    }

    private LocalDate eventDay(Event event) {
        return event.getTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate();
    }

    private void writeResponse(BufferedWriter writer, DhtProtocol.Response response) {
        try {
            writer.write(mapper.writeValueAsString(response));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }
}
