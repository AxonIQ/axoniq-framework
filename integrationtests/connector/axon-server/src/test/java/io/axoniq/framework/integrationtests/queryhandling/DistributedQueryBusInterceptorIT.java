/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.integrationtests.queryhandling;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.messaging.queryhandling.distributed.DistributedQueryBus;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.integrationtests.queryhandling.AbstractQueryInterceptorTestSuite;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

/**
 * An {@link AbstractQueryInterceptorTestSuite} implementation validating query interceptor functionality with the
 * {@link DistributedQueryBus}.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class DistributedQueryBusInterceptorIT extends AbstractQueryInterceptorTestSuite {

    protected static final Logger logger = LoggerFactory.getLogger(DistributedQueryBusInterceptorIT.class);

    /*
     * A context of its own, rather than the shared container's default context, so that this suite doesn't race
     * DistributedQueryBusSubscriptionQueryIT (same module, can run in a concurrent fork) recreating "default" too.
     */
    private static final String CONTEXT = "distributed-query-bus-interceptor-it";

    private static final AxonServerContainer container = SharedAxonServerContainer.INSTANCE;

    private static AxonConfiguration config;

    @BeforeAll
    static void beforeAll() throws IOException {
        SharedAxonServerContainer.ensureStarted();

        try {
            AxonServerContainerUtils.deleteContext(container.getHost(), container.getHttpPort(), CONTEXT);
        } catch (IOException ignored) {
            // Context didn't exist yet.
        }
        AxonServerContainerUtils.createContext(container.getHost(),
                                               container.getHttpPort(),
                                               CONTEXT,
                                               AxonServerContainerUtils.DCB_CONTEXT);
        logger.info("Using Axon Server for integration test. UI is available at http://localhost:{}",
                    container.getHttpPort());

        // One connection shared by every test method in this class, rather than one per method: opening and
        // tearing down a fresh Axon Server connection per test churns through client registrations faster than the
        // server's async disconnect processing can keep up, which can transiently exceed a license's connection
        // limit under CI load. Test query/handler names are already UUID-scoped, so sharing one connection across
        // methods is safe.
        config = buildConfigurer().build();
        awaitQueryRoutingReady(config);
    }

    private static AxonServerConfiguration testContainerAxonServerConfiguration() {
        AxonServerConfiguration axonServerConfiguration = new AxonServerConfiguration();
        axonServerConfiguration.setServers(container.getHost() + ":" + container.getGrpcPort());
        axonServerConfiguration.setContext(CONTEXT);
        return axonServerConfiguration;
    }

    private static MessagingConfigurer buildConfigurer() {
        return MessagingConfigurer.create()
                                  .componentRegistry(cr -> cr.registerComponent(
                                          AxonServerConfiguration.class,
                                          c -> testContainerAxonServerConfiguration()
                                  ));
    }

    @Override
    public QueryBus queryBus() {
        return config.getComponent(QueryBus.class);
    }

    @Override
    protected MessagingConfigurer createMessagingConfigurer() {
        return buildConfigurer();
    }

    /**
     * A freshly-opened connection's query stream can briefly lag behind the connection itself becoming usable:
     * {@code registerQueryHandler} always succeeds locally, but the server-side acknowledgment over the stream can
     * silently fail if issued before the stream is actually up (only resolved later, asynchronously, when the
     * connector reconnects and resubscribes). Test methods here don't know that and block on an untimed
     * {@code join()} for a response that then never arrives. Retrying a throwaway registration on this same
     * connection, and blocking until it stops failing, closes that race before any real test runs.
     */
    private static void awaitQueryRoutingReady(AxonConfiguration configuration) {
        QueryBus queryBus = configuration.getComponent(QueryBus.class);
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofMillis(500))
               .ignoreExceptions()
               .untilAsserted(() -> queryBus.subscribe(new QualifiedName("warmup"),
                                                       (query, context) -> MessageStream.empty()));
    }

    @AfterAll
    static void tearDown() {
        config.shutdown();
    }
}
