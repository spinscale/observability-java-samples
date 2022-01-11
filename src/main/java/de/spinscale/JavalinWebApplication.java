package de.spinscale;

import co.elastic.apm.attach.ElasticApmAttacher;
import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.javalin.Javalin;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class JavalinWebApplication {

    public static void main(String argv[]) throws Exception {
        // allows to find this in the overview in the APM UI, otherwise the class name will be used
        System.setProperty("elastic.apm.service_name", "sample-webserver");
        System.setProperty("elastic.apm.application_packages", "de.spinscale");
        System.setProperty("elastic.apm.enable_log_correlation", "true");
        ElasticApmAttacher.attach();

        JavalinWebApplication application = new JavalinWebApplication();
        application.run();
    }

    private final Javalin app;
    private final RestClientTransport transport;
    private final ElasticsearchClient client;

    public JavalinWebApplication() {
        this.app = Javalin.create();
        // proper shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));

        // create ES client
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);

        // template rendering endpoint
        app.get("/", ctx -> {
            String user = ctx.queryParam("user");
            user = user == null ? "world" : user;
            ctx.status(200).render("main.jte", Map.of("user", user));
        });

        // elasticsearch index operation
        app.put("/index/{id}", ctx -> {
            final String id = ctx.pathParam("id");
            final Product product = ctx.bodyAsClass(Product.class);
            final IndexResponse response = client.index(builder -> builder.index("my-index").id(id).document(product));
            ctx.result(response.result().jsonValue());
        });

        // elasticsearch get
        app.get("/index/{id}", ctx -> {
            final String id = ctx.pathParam("id");
            final GetResponse<Product> objectGetResponse = client.get(builder -> builder.index("my-index").id(id), Product.class);
            if (objectGetResponse.found()) {
                ctx.json(objectGetResponse.source());
            } else {
                ctx.status(404);
            }
        });

        final ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);
        app.get("/search", ctx -> {
            final CompletableFuture<List<Product>> response = asyncClient.search(builder -> builder
                    .index("my-index")
                    .query(qb -> qb
                            .queryString(qs -> qs
                                    .query(ctx.queryParam("query")
                                    )
                            )
                    ), Product.class)
                    .thenApply(r -> r.hits().hits().stream().map(Hit::source).toList());

            ctx.future(response);
        });
    }

    public void run() {
        app.start(8080);
    }

    public void close() {
        app.close();
        try {
            transport.close();
        } catch (IOException e) {
        }

    }

    public static class Product {
        private String title;
        private String description;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }
}
