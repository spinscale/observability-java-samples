package de.spinscale;

import co.elastic.apm.api.ElasticApm;
import co.elastic.apm.api.Scope;
import co.elastic.apm.api.Span;
import co.elastic.apm.api.Traced;
import co.elastic.apm.attach.ElasticApmAttacher;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class ApmHttpClientExample {

    public static void main(String argv[]) throws Exception {
        // allows to find this in the overview in the APM UI, otherwise the class name will be used
        System.setProperty("elastic.apm.service_name", "http-downloader");
        System.setProperty("elastic.apm.application_packages", "de.spinscale");
        ElasticApmAttacher.attach();

        final ApmHttpClientExample example = new ApmHttpClientExample();
        example.run();
    }

    private final HttpClient client;

    public ApmHttpClientExample() throws Exception {
        this.client = HttpClient.newHttpClient();
    }

    public void run() throws Exception {
        // run this for a couple of times, then wait for a bit
        for (int i = 0; i < 200; i++) {
            System.out.println("Run " + (i+1) + "/10");
            doTasks();
            Thread.sleep(10_000);
        }
    }

    @Traced("run-all-tasks")
    public void doTasks() throws Exception {
        final HttpRequest req = HttpRequest.newBuilder(URI.create("https://elastic.co")).build();

        // sync HTTP request
        client.send(req, HttpResponse.BodyHandlers.discarding());

        // sync task with annotation
        syncTaskWithAnnotation();

        // async HTTP request
        final CompletableFuture<HttpResponse<Void>> future = client.sendAsync(req, HttpResponse.BodyHandlers.discarding());
        future.get();

        // async task with programmatic span
        asyncTaskProgrammaticSpan();
    }

    @Traced("sync-task-annotation")
    public void syncTaskWithAnnotation() {
        try {
            Thread.sleep(300);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void asyncTaskProgrammaticSpan() throws Exception {
        final Span span = ElasticApm.currentTransaction().startSpan();
        span.setName("async-task-programmatic");
        CompletableFuture.runAsync(() -> {
            try (final Scope scope = span.activate()) {
                try {
                    Thread.sleep(300);
                } catch (Exception e) {}
                span.end();
            }
        }).get();
    }
}