package pt.ua;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;

public class ReactiveNodeClient {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public Mono<Void> ingest(NodeInfo node, Event event, String indexField) {
        try {
            String json = mapper.writeValueAsString(new IngestRequest(event, indexField));
            return RSocketConnector.create()
                    .connect(TcpClientTransport.create(node.getHost(), node.getPort()))
                    .flatMap(socket ->
                            socket.fireAndForget(DefaultPayload.create(json))
                                    .then(Mono.fromRunnable(socket::dispose))
                    );
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    public Flux<Event> streamSubSeries(NodeInfo node, LocalDate day, String indexField, String indexValue) {
        try {
            String json = mapper.writeValueAsString(new QueryRequest(day.toString(), indexField, indexValue));
            return RSocketConnector.create()
                    .connect(TcpClientTransport.create(node.getHost(), node.getPort()))
                    .flatMapMany(socket ->
                            socket.requestStream(DefaultPayload.create(json))
                                    .map(payload -> {
                                        try {
                                            return mapper.readValue(payload.getDataUtf8(), QueryResponse.class);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    })
                                    .filter(resp -> "EVENT".equals(resp.type) && resp.event != null)
                                    .map(resp -> resp.event)
                                    .doFinally(sig -> socket.dispose())
                    );
        } catch (Exception e) {
            return Flux.error(e);
        }
    }

    static class IngestRequest {
        public Event event;
        public String indexField;

        public IngestRequest() {}
        public IngestRequest(Event event, String indexField) {
            this.event = event;
            this.indexField = indexField;
        }
    }

    static class QueryRequest {
        public String day;
        public String indexField;
        public String indexValue;

        public QueryRequest() {}
        public QueryRequest(String day, String indexField, String indexValue) {
            this.day = day;
            this.indexField = indexField;
            this.indexValue = indexValue;
        }
    }

    static class QueryResponse {
        public String requestId;
        public String type;
        public Event event;
        public String error;

        public QueryResponse() {}
    }
}