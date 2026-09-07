package io.axoniq.shardlab;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * One workflow engine node. Started as its own OS process by the tests so that a {@code kill -9} is a real crash.
 */
@SpringBootApplication
public class NodeApp {

    public static void main(String[] args) {
        SpringApplication.run(NodeApp.class, args);
    }

    /**
     * The workflow event processor lives in a nested module configuration, so it has to be searched for; the root
     * {@code Configuration} does not expose it.
     */
    static Optional<StreamingEventProcessor> findProcessor(Configuration configuration) {
        var direct = configuration.getOptionalComponent(StreamingEventProcessor.class, "Workflow");
        if (direct.isPresent()) {
            return direct;
        }
        for (var module : configuration.getModuleConfigurations()) {
            var found = findProcessor(module);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    @Bean
    public ApplicationListener<ApplicationReadyEvent> readyPrinter(Configuration configuration, StepLog stepLog) {
        return event -> {
            var nodeId = System.getProperty("shardlab.node", "?");
            System.out.println("PROCESSOR_FOUND " + findProcessor(configuration).isPresent());
            System.out.println("NODE_READY node=" + nodeId + " pid=" + ProcessHandle.current().pid());
            System.out.flush();
            Thread reporter = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(500);
                        var processor = findProcessor(configuration).orElseThrow();
                        var owned = processor.processingStatus().keySet().stream()
                                             .sorted()
                                             .map(String::valueOf)
                                             .collect(Collectors.joining(","));
                        stepLog.recordOwnership(nodeId, owned);
                        System.out.println("SEGMENTS node=" + nodeId + " owned=[" + owned + "]");
                        System.out.flush();
                    } catch (InterruptedException e) {
                        return;
                    } catch (Exception e) {
                        System.out.println("SEGMENTS node=" + nodeId + " error=" + e);
                    }
                }
            }, "segment-reporter");
            reporter.setDaemon(true);
            reporter.start();
        };
    }
}
