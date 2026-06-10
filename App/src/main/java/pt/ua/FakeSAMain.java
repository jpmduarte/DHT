package pt.ua;

import io.reactivex.rxjava3.core.Flowable;
import java.time.LocalDate;


public class FakeSAMain {
    
    // Classe auxiliar para acumular agregação
    static class AggregationAccumulator {
        long count = 0;
        double sum = 0;
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
    }


    public static void main(String[] args) {
        ReactiveNodeClient client = new ReactiveNodeClient();
        String host = args.length >= 1 ? args[0] : "localhost";
        int port = args.length >= 2 ? Integer.parseInt(args[1]) : 7878;
        NodeInfo bootstrap = new NodeInfo("bootstrap", host, port);


        LocalDate minDay = LocalDate.of(2026, 6, 1);
        LocalDate maxDay = LocalDate.of(2026, 6, 9);
        String indexField = "zone";
        String indexValue = "eu";


        queryRangeAndAggregate(client, bootstrap, minDay, maxDay, indexField, indexValue);
    }


    private static void queryRangeAndAggregate(ReactiveNodeClient client,
                                               NodeInfo bootstrap,
                                               LocalDate minDay,
                                               LocalDate maxDay,
                                               String indexField,
                                               String indexValue) {
        AggregationAccumulator acc = new AggregationAccumulator();


        System.out.println("[sa] Query RANGE: " + minDay + " to " + maxDay +
                ", field=" + indexField + ", value=" + indexValue);


        // ONLY ONE call to the AST, sem loop no SA
        Flowable<Event> stream = client.streamSubSeriesRange(bootstrap, minDay, maxDay, indexField, indexValue);


        stream.blockingForEach(ev -> {
            acc.count++;
            String tempStr = ev.getField("temperature");
            if (tempStr != null) {
                try {
                    double temp = Double.parseDouble(tempStr);
                    acc.sum += temp;
                    if (temp > acc.max) acc.max = temp;
                    if (temp < acc.min) acc.min = temp;
                } catch (NumberFormatException ignored) {
                }
            }
        });


        System.out.println("[sa] === Aggregation results ===");
        System.out.println("[sa] COUNT: " + acc.count);
        if (acc.count > 0) {
            System.out.println("[sa] SUM: " + acc.sum);
            System.out.println("[sa] AVG: " + (acc.sum / acc.count));
            System.out.println("[sa] MAX: " + acc.max);
            System.out.println("[sa] MIN: " + acc.min);
        } else {
            System.out.println("[sa] No events found for this range and filter.");
        }
    }
}