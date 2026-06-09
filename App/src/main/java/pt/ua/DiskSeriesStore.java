package pt.ua;

import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class DiskSeriesStore {
    private final Path baseDir;

    public DiskSeriesStore(Path baseDir) {
        this.baseDir = baseDir;
    }

    public Path pathFor(SeriesKey key) {
        String fileName = key.getDay() + "__" + key.getIndexField() + "__" + key.getIndexValue() + ".log";
        return baseDir.resolve(fileName);
    }

    public synchronized void append(Event event, SeriesKey key) throws IOException {
        Files.createDirectories(baseDir);
        Path path = pathFor(key);

        try (BufferedWriter writer = Files.newBufferedWriter(
                path,
                StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND)) {
            writer.write(JsonEventCodec.toJson(event));
            writer.newLine();
        }

        System.out.println("[disk] appended to " + path.toAbsolutePath());
    }

    public Flux<Event> streamAll(SeriesKey key) {
        Path path = pathFor(key);
        if (!Files.exists(path)) {
            System.out.println("[disk] no file for " + key + " at " + path.toAbsolutePath());
            return Flux.empty();
        }

        System.out.println("[disk] reading " + path.toAbsolutePath());

        return Flux.using(
                () -> Files.newBufferedReader(path, StandardCharsets.UTF_8),
                br -> Flux.generate(sink -> {
                    try {
                        String line = br.readLine();
                        if (line == null) {
                            sink.complete();
                        } else {
                            sink.next(JsonEventCodec.fromJson(line));
                        }
                    } catch (IOException e) {
                        sink.error(e);
                    }
                }),
                br -> {
                    try {
                        br.close();
                    } catch (IOException ignored) {
                    }
                }
        );
    }
}