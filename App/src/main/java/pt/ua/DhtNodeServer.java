package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.UUID;

public class DhtNodeServer {
    private final NodeState state;
    private final NodeInfo self;
    private final PeerTable peers;
    private final ReactiveNodeClient client;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public DhtNodeServer(NodeState state, NodeInfo self, PeerTable peers) {
        this.state = state;
        this.self = self;
        this.peers = peers;
        this.client = new ReactiveNodeClient();
    }

    public void start() {
        System.out.println("[server] Starting DHT node=" + self.getNodeId() + " on port=" + self.getPort());

        RSocketServer.create((setup, sendingSocket) -> Mono.just(new RSocket() {

                    @Override
                    public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.<Void>fromRunnable(() -> handleIngest(payload))
                                .onErrorResume(e -> Mono.<Void>empty());
                    }

                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        try {
                            return handleQuery(payload)
                                    .onErrorResume(e -> Flux.just(
                                            DefaultPayload.create(
                                                    toJsonSafe(QueryResponse.error(UUID.randomUUID().toString(), e.getMessage()))
                                            )
                                    ));
                        } catch (Exception e) {
                            return Flux.just(DefaultPayload.create(
                                    toJsonSafe(QueryResponse.error(UUID.randomUUID().toString(), e.getMessage()))
                            ));
                        }
                    }

                    private void handleIngest(Payload payload) {
                        try {
                            ReactiveNodeClient.IngestRequest req =
                                    mapper.readValue(payload.getDataUtf8(), ReactiveNodeClient.IngestRequest.class);

                            Event event = req.event;
                            String indexField = req.indexField;
                            String indexValue = event.getField(indexField);
                            if (indexValue == null) return;

                            LocalDate day = event.getTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate();
                            String shardKey = day + "|" + indexField + "|" + indexValue;
                            BigInteger targetId = KeyHasher.hashToId(shardKey);

                            NodeInfo owner = responsibleNode(targetId);
                            if (owner == null) return;

                            if (owner.getNodeId().equals(self.getNodeId())) {
                                state.ingest(event, indexField);
                                return;
                            }

                            client.ingest(owner, event, indexField).subscribe();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    private Flux<Payload> handleQuery(Payload payload) {
                        try {
                            ReactiveNodeClient.QueryRequest req =
                                    mapper.readValue(payload.getDataUtf8(), ReactiveNodeClient.QueryRequest.class);

                            LocalDate day = LocalDate.parse(req.day);
                            String shardKey = day + "|" + req.indexField + "|" + req.indexValue;
                            BigInteger targetId = KeyHasher.hashToId(shardKey);

                            NodeInfo owner = responsibleNode(targetId);
                            if (owner == null) {
                                return Flux.just(DefaultPayload.create(
                                        toJsonSafe(QueryResponse.error(UUID.randomUUID().toString(), "No owner found"))
                                ));
                            }

                            String requestId = UUID.randomUUID().toString();

                            Flux<Event> source = owner.getNodeId().equals(self.getNodeId())
                                    ? state.streamSubSeries(day, req.indexField, req.indexValue)
                                    : client.streamSubSeries(owner, day, req.indexField, req.indexValue);

                            return source
                                    .map(ev -> DefaultPayload.create(toJsonSafe(QueryResponse.event(requestId, ev))))
                                    .concatWith(Mono.fromSupplier(() ->
                                            DefaultPayload.create(toJsonSafe(QueryResponse.complete(requestId)))
                                    ).flux());
                        } catch (Exception e) {
                            return Flux.just(DefaultPayload.create(
                                    toJsonSafe(QueryResponse.error(UUID.randomUUID().toString(), e.getMessage()))
                            ));
                        }
                    }

                    private NodeInfo responsibleNode(BigInteger targetId) {
                        NodeInfo best = self;
                        BigInteger bestDist = KeyHasher.xorDistance(KeyHasher.hashToId(self.getNodeId()), targetId);

                        for (NodeInfo peer : peers.allPeers()) {
                            if (peer.getNodeId().equals(self.getNodeId())) continue;
                            BigInteger dist = KeyHasher.xorDistance(KeyHasher.hashToId(peer.getNodeId()), targetId);
                            if (dist.compareTo(bestDist) < 0) {
                                best = peer;
                                bestDist = dist;
                            }
                        }
                        return best;
                    }

                    private String toJsonSafe(Object value) {
                        try {
                            return mapper.writeValueAsString(value);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }))
                .bind(TcpServerTransport.create(self.getPort()))
                .block()
                .onClose()
                .block();
    }

    static class QueryResponse {
        public String requestId;
        public String type;
        public Event event;
        public String error;

        public QueryResponse() {}

        static QueryResponse event(String requestId, Event event) {
            QueryResponse r = new QueryResponse();
            r.requestId = requestId;
            r.type = "EVENT";
            r.event = event;
            return r;
        }

        static QueryResponse complete(String requestId) {
            QueryResponse r = new QueryResponse();
            r.requestId = requestId;
            r.type = "COMPLETE";
            return r;
        }

        static QueryResponse error(String requestId, String error) {
            QueryResponse r = new QueryResponse();
            r.requestId = requestId;
            r.type = "ERROR";
            r.error = error;
            return r;
        }
    }
}