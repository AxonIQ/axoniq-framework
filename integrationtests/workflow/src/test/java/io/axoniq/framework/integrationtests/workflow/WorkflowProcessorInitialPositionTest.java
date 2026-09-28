package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.workflow.annotation.Workflow;
import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The engine's event processor starts at the head of the stream. Start events that were in the store before the
 * engine first started do not start workflows: they belong to whatever process handled them before this deployment,
 * for example a saga that is being replaced by this workflow.
 */
class WorkflowProcessorInitialPositionTest {

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void startEventsPublishedBeforeTheFirstStartDoNotStartWorkflows() throws Exception {
        // given a workflow definition and a start event that is already in the store
        var started = ConcurrentHashMap.<String>newKeySet();
        var tokenStore = new InMemoryTokenStore();
        var configurer = WorkflowConfigurer.create();
        configurer.componentRegistry(r -> r.disableEnhancer(AxonServerConfigurationEnhancer.class)
                                           .registerComponent(TokenStore.class, c -> tokenStore));
        configurer.registerWorkflowModule(WorkflowModule.defaults("Recording", SimpleWorkflowContext.class)
                                                        .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                                        .definition(d -> d.autodetected(c -> new RecordingWorkflow(started))));
        configuration = configurer.build();
        var events = configuration.getComponent(EventGateway.class);
        events.publish(null, List.of(new ThingRequested("before"))).orTimeout(5, TimeUnit.SECONDS).join();

        // when the engine starts for the first time and has claimed its segments (the initial token is stored then)
        configuration.start();
        await().atMost(Duration.ofSeconds(15)).until(() -> storedToken(tokenStore) != null);

        // then the initial token is a head position, not a replay from the first event
        assertThat(storedToken(tokenStore)).as("initial token of the workflow processor").isNotInstanceOf(ReplayToken.class);

        // and the historical start event does not start a workflow
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(started).as("history must not start workflows").doesNotContain("before"));
    }

    private static TrackingToken storedToken(InMemoryTokenStore tokenStore) {
        try {
            return tokenStore.fetchToken("Workflow", 0, null).orTimeout(5, TimeUnit.SECONDS).join();
        } catch (Exception notInitializedYet) {
            return null;
        }
    }

    public record ThingRequested(String id) {
    }

    public static class RecordingWorkflow {

        private final Set<String> started;

        RecordingWorkflow(Set<String> started) {
            this.started = started;
        }

        @Workflow(workflowName = "Recording", workflowNamespace = "io.axoniq.test.initialposition",
                  idProperty = "id", startOnEventClass = ThingRequested.class)
        public void execute(SimpleWorkflowContext ctx) {
            started.add((String) ctx.workflowPayload().get("id"));
        }
    }
}
