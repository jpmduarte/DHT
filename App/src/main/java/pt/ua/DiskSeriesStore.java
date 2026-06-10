package pt.ua;

import io.reactivex.rxjava3.core.Flowable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class DiskSeriesStore {
    private final Path baseDir;

    public DiskSeriesStore(Path baseDir) {
        this.baseDir = baseDir;
    }

    public Path pathFor(SeriesKey key) {
        String fileName = key.getDay() + "__" + encodePathPart(key.getIndexField()) + "__"
                + encodePathPart(key.getIndexValue()) + ".log";
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

    public Flowable<Event> streamAll(SeriesKey key) {
        Path path = pathFor(key);
        if (!Files.isRegularFile(path)) {
            System.out.println("[disk] no file for " + key + " at " + path.toAbsolutePath());
            return Flowable.empty();
        }

        System.out.println("[disk] reading " + path.toAbsolutePath());

        return Flowable.using(
                () -> Files.newBufferedReader(path, StandardCharsets.UTF_8),
                br -> Flowable.generate(emitter -> {
                    try {
                        String line = br.readLine();
                        if (line == null) {
                            emitter.onComplete();
                        } else {
                            emitter.onNext(JsonEventCodec.fromJson(line));
                        }
                    } catch (IOException | RuntimeException e) {
                        emitter.onError(e);
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

    private String encodePathPart(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
