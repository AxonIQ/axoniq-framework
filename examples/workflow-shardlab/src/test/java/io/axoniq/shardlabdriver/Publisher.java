package io.axoniq.shardlabdriver;

import io.axoniq.shardlab.Events;

import io.axoniq.framework.workflow.springboot.WorkflowAutoConfiguration;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Publishes business events into Axon Server without ever becoming a workflow node: the workflow autoconfiguration
 * is excluded, so this process never registers the {@code Workflow} processor and never claims a segment.
 */
public final class Publisher implements AutoCloseable {

    @SpringBootApplication(exclude = WorkflowAutoConfiguration.class,
                           scanBasePackages = "io.axoniq.shardlabdriver.none")
    static class PublisherApp {

    }

    private final ConfigurableApplicationContext context;
    private final EventSink eventSink;
    private final MessageTypeResolver typeResolver;
    private final EventConverter converter;

    public Publisher(String axonServer, String jdbcUrl, String user, String password) {
        this.context = new SpringApplication(PublisherApp.class).run(
                "--spring.main.web-application-type=none",
                "--spring.main.banner-mode=off",
                "--logging.level.root=WARN",
                "--axon.axonserver.servers=" + axonServer,
                "--spring.datasource.url=" + jdbcUrl,
                "--spring.datasource.username=" + user,
                "--spring.datasource.password=" + password
        );
        this.eventSink = context.getBean(EventSink.class);
        this.typeResolver = context.getBean(MessageTypeResolver.class);
        this.converter = context.getBean(EventConverter.class);
    }

    public void publish(Object payload) {
        var message = new GenericEventMessage(typeResolver.resolveOrThrow(payload), payload)
                .withConverter(converter);
        eventSink.publish(null, message).join();
    }

    @Override
    public void close() {
        context.close();
    }

    /** Manual driver: {@code PublisherMain <axonServer> <jdbcUrl> <user> <pass> order:o-1 payment:o-1 ...}. */
    public static void main(String[] args) {
        try (var publisher = new Publisher(args[0], args[1], args[2], args[3])) {
            for (var i = 4; i < args.length; i++) {
                var parts = args[i].split(":", 2);
                switch (parts[0]) {
                    case "order" -> publisher.publish(new Events.OrderPlaced(parts[1]));
                    case "payment" -> publisher.publish(new Events.PaymentReceived(parts[1]));
                    case "slow" -> publisher.publish(new Events.SlowJobRequested(parts[1], 30));
                    case "flaky" -> publisher.publish(new Events.FlakyJobRequested(parts[1], 3));
                    default -> throw new IllegalArgumentException("unknown: " + args[i]);
                }
                System.out.println("PUBLISHED " + args[i]);
            }
        }
        System.exit(0);
    }
}
