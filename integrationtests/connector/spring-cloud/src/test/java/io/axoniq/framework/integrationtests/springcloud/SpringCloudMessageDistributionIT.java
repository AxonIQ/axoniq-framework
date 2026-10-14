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

package io.axoniq.framework.integrationtests.springcloud;

import io.axoniq.framework.integrationtests.springcloud.SpringCloudNodes.CreateCourse;
import io.axoniq.framework.integrationtests.springcloud.SpringCloudNodes.FindCourse;
import io.axoniq.framework.integrationtests.springcloud.SpringCloudNodes.FindCourseCatalog;
import io.axoniq.framework.integrationtests.springcloud.SpringCloudNodes.RenameCourse;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesPayload;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.query.SubscriptionQueryMembersChangedException;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberDiscovery;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.junit.jupiter.api.*;
import org.awaitility.Awaitility;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Duration;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A two-node test of the Spring Cloud connector, distributing messages over HTTP between two Spring Boot applications
 * in one JVM.
 * <p>
 * Both nodes handle the same message, so where a message is handled is decided by the routing ring alone. Each node's
 * handler answers with its own name, which is how the test observes routing without reaching into either node.
 *
 * @author Allard Buijze
 */
class SpringCloudMessageDistributionIT {

    private static final String NODE_A = "node-a";
    private static final String NODE_B = "node-b";
    private static final int COMMAND_COUNT = 40;

    private int portA;
    private int portB;
    private ConfigurableApplicationContext nodeA;
    private ConfigurableApplicationContext nodeB;
    private RestClient restClient;

    @BeforeEach
    void setUp() throws IOException {
        portA = freePort();
        portB = freePort();
        // Ports are reserved before either node starts, because each node has to be told the whole cluster's
        // addresses up front, as there is no registry here to discover them from.
        nodeA = startNode(NODE_A, portA);
        nodeB = startNode(NODE_B, portB);
        restClient = RestClient.create();
        converge();
    }

    @AfterEach
    void tearDown() {
        if (nodeA != null) {
            nodeA.close();
        }
        if (nodeB != null) {
            nodeB.close();
        }
    }

    private ConfigurableApplicationContext startNode(String nodeName, int port) {
        return new SpringApplicationBuilder(SpringCloudNodes.NodeApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties("server.port=" + port,
                            "test.node.name=" + nodeName,
                            "test.cluster.ports=" + portA + "," + portB,
                            "test.query-handler.enabled=" + NODE_B.equals(nodeName),
                            "test.rename-handler.enabled=" + NODE_B.equals(nodeName),
                            // The test's discovery keeps reporting a node after it stops, as a registry does until it
                            // evicts it, so a stopped node is only noticed once its capabilities are refreshed.
                            "axon.springcloud.capabilities-refresh-interval=1s",
                            "axon.multitenancy.enabled=false",
                            "axon.axonserver.enabled=false",
                            "spring.main.banner-mode=off",
                            "logging.level.root=WARN")
                .run();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Drives a discovery round on every running node and waits until each has the expected number of members.
     * <p>
     * The connector rebuilds its ring on a {@link HeartbeatEvent}, which a real discovery implementation publishes on
     * its own schedule. Publishing it here is what a heartbeat would do, without waiting on one.
     */
    private void converge(ConfigurableApplicationContext... nodes) {
        ConfigurableApplicationContext[] running = nodes.length == 0
                ? new ConfigurableApplicationContext[]{nodeA, nodeB}
                : nodes;
        for (ConfigurableApplicationContext node : running) {
            node.publishEvent(new HeartbeatEvent(this, System.nanoTime()));
        }
        Awaitility.await()
                  .atMost(Duration.ofSeconds(20))
                  .untilAsserted(() -> {
                      for (ConfigurableApplicationContext node : running) {
                          node.publishEvent(new HeartbeatEvent(this, System.nanoTime()));
                          assertThat(handlingMembersOf(node)).hasSize(running.length);
                      }
                  });
    }

    private static Set<Member> handlingMembersOf(ConfigurableApplicationContext node) {
        SpringCloudMemberDiscovery discovery = node.getBean(SpringCloudMemberDiscovery.class);
        return discovery.members().stream()
                       .filter(member -> discovery.capabilitiesOf(member)
                                                 .filter(capabilities -> !capabilities.commands().isEmpty())
                                                 .isPresent())
                       .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static List<String> handleCoursesFrom(ConfigurableApplicationContext node, int count) {
        CommandGateway gateway = node.getBean(CommandGateway.class);
        List<String> handledBy = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            handledBy.add(gateway.sendAndWait(new CreateCourse("course-" + i, "Axon 5"), String.class));
        }
        return handledBy;
    }

    private static List<String> findCoursesFrom(ConfigurableApplicationContext node, int count) {
        QueryGateway gateway = node.getBean(QueryGateway.class);
        List<String> handledBy = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            handledBy.add(gateway.query(new FindCourse("course-" + i), String.class, null)
                                  .orTimeout(20, TimeUnit.SECONDS)
                                  .join());
        }
        return handledBy;
    }

    @Nested
    class DistributingCommands {

        @Test
        void distributesCommandsAcrossBothNodes() {
            // when
            List<String> handledBy = handleCoursesFrom(nodeA, COMMAND_COUNT);

            // then commands leave the node they were dispatched from, which is the whole point of the connector
            assertThat(Set.copyOf(handledBy)).containsExactlyInAnyOrder(NODE_A, NODE_B);
        }

        @Test
        void routesTheSameCourseToTheSameNode() {
            // given
            CommandGateway gateway = nodeA.getBean(CommandGateway.class);

            // when the same routing key, dispatched repeatedly
            Set<String> handlers = IntStream.range(0, 20)
                                            .mapToObj(i -> gateway.sendAndWait(
                                                    new CreateCourse("course-7", "Axon 5"), String.class
                                            ))
                                            .collect(Collectors.toSet());

            // then one course is handled on one node, which is what keeps its commands serialized
            assertThat(handlers).hasSize(1);
        }

        @Test
        void routesTheSameCourseToTheSameNodeFromEitherNode() {
            // when the same routing key, dispatched from each node in turn
            CommandGateway fromA = nodeA.getBean(CommandGateway.class);
            CommandGateway fromB = nodeB.getBean(CommandGateway.class);
            String viaA = fromA.sendAndWait(new CreateCourse("course-7", "Axon 5"), String.class);
            String viaB = fromB.sendAndWait(new CreateCourse("course-7", "Axon 5"), String.class);

            // then if the two nodes disagreed on where a key belongs, one course would be handled in two places
            assertThat(viaA).isEqualTo(viaB);
        }
    }

    @Nested
    class DistributingQueries {

        @Test
        void routesAQueryToTheMemberAdvertisingItsHandler() {
            // when
            List<String> handledBy = findCoursesFrom(nodeA, COMMAND_COUNT);

            // then node A has no local handler, so the distributed query bus must use node B's advertisement
            assertThat(Set.copyOf(handledBy)).containsExactly(NODE_B);
        }

        @Test
        void emitsSubscriptionUpdatesFromAnEventHandlerContext() {
            // given
            QueryBus queryBus = nodeA.getBean(QueryBus.class);
            MessageStream<QueryResponseMessage> responses = queryBus.subscriptionQuery(
                    new GenericQueryMessage(new MessageType(FindCourse.class), new FindCourse("course-1")),
                    null,
                    16
            );
            try {
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(responses::hasNextAvailable);
                assertThat(responses.next().orElseThrow().message().payloadAs(String.class)).isEqualTo(NODE_B);

                // when
                CommandGateway gateway = nodeA.getBean(CommandGateway.class);
                assertThat(gateway.sendAndWait(new RenameCourse("course-1", "Axon 5 renamed"), String.class))
                        .isEqualTo(NODE_B);

                // then the event handler must resolve the same distributed query bus that owns the subscription
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(responses::hasNextAvailable);
                assertThat(responses.next().orElseThrow().message().payloadAs(String.class))
                        .isEqualTo(NODE_B + "-renamed");
            } finally {
                responses.close();
            }
        }
    }

    @Nested
    class DistributingSubscriptionQueries {

        private static final QualifiedName FIND_COURSE = new MessageType(FindCourse.class).qualifiedName();
        private static final QualifiedName FIND_COURSE_CATALOG =
                new MessageType(FindCourseCatalog.class).qualifiedName();

        @Test
        void answersASubscriptionQueryWithItsInitialResultAndThenTheUpdates() {
            // given a subscription opened from the node that does not handle the query
            Subscriber subscriber = subscribe(nodeA, new FindCourse("course-1"));

            try {
                // when the initial result has arrived and the handling node emits an update
                subscriber.awaitReceived(1);
                emitUpdateOn(nodeB, FIND_COURSE, "course-1@" + NODE_B);

                // then it crosses the wire onto the same stream the initial result arrived on
                subscriber.awaitReceived(2);
                assertThat(subscriber.received).containsExactly(NODE_B, "course-1@" + NODE_B);
            } finally {
                subscriber.dispose();
            }
        }

        @Test
        void carriesTheUpdatesOfEveryNodeHandlingTheQuery() {
            // given a subscription to a query both nodes handle, only one of which answers the initial result
            Subscriber subscriber = subscribe(nodeA, new FindCourseCatalog("computer-science"));

            try {
                subscriber.awaitReceived(1);

                // when each node emits an update, as each does for the state it changed
                emitUpdateOn(nodeA, FIND_COURSE_CATALOG, "catalog@" + NODE_A);
                emitUpdateOn(nodeB, FIND_COURSE_CATALOG, "catalog@" + NODE_B);

                // then the updates of the node that did not answer the initial result arrive too
                subscriber.awaitReceived(3);
                assertThat(subscriber.received.subList(1, 3))
                        .containsExactlyInAnyOrder("catalog@" + NODE_A, "catalog@" + NODE_B);
            } finally {
                subscriber.dispose();
            }
        }

        @Test
        void deliversTheUpdatesToASubscriberOfTheUpdatesAlone() {
            // given
            MessageStream<SubscriptionQueryUpdateMessage> updates = nodeA.getBean(QueryBus.class).subscribeToUpdates(
                    new GenericQueryMessage(new MessageType(FindCourse.class), new FindCourse("course-1")), 16
            );

            try {
                // when the handling node emits an update, repeated because nothing says when the subscription has
                // reached it: a subscriber to the updates alone receives no initial result to wait on
                List<String> received = new ArrayList<>();
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                    emitUpdateOn(nodeB, FIND_COURSE, "course-1@" + NODE_B);
                    while (updates.hasNextAvailable()) {
                        updates.next().ifPresent(entry -> received.add(entry.message().payloadAs(String.class)));
                    }
                    return !received.isEmpty();
                });

                // then the updates arrive, told apart from the initial result that was left out
                assertThat(received).containsOnly("course-1@" + NODE_B);
            } finally {
                updates.close();
            }
        }

        @Test
        void completesTheSubscriptionWhenTheHandlingNodeCompletesIt() {
            // given
            Subscriber subscriber = subscribe(nodeA, new FindCourse("course-1"));

            try {
                subscriber.awaitReceived(1);

                // when
                nodeB.getBean(QueryBus.class)
                     .completeSubscriptions(query -> FIND_COURSE.equals(query.type().qualifiedName()), null)
                     .orTimeout(20, TimeUnit.SECONDS)
                     .join();

                // then
                Awaitility.await().atMost(Duration.ofSeconds(20)).untilTrue(subscriber.completed);
                assertThat(subscriber.failure).hasNullValue();
            } finally {
                subscriber.dispose();
            }
        }

        @Test
        void failsTheSubscriptionWhenTheHandlingNodeFailsIt() {
            // given
            Subscriber subscriber = subscribe(nodeA, new FindCourse("course-1"));

            try {
                subscriber.awaitReceived(1);

                // when
                nodeB.getBean(QueryBus.class)
                     .completeSubscriptionsExceptionally(
                             query -> FIND_COURSE.equals(query.type().qualifiedName()),
                             new IllegalStateException("The course catalog is being rebuilt."),
                             null
                     )
                     .orTimeout(20, TimeUnit.SECONDS)
                     .join();

                // then
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> subscriber.failure.get() != null);
                assertThat(subscriber.failure.get()).hasMessageContaining("The course catalog is being rebuilt.");
            } finally {
                subscriber.dispose();
            }
        }

        @Test
        void failsTheSubscriptionWhenANodeStartsHandlingTheQuery() {
            // given a subscription opened while only node A handles the query
            nodeB.close();
            nodeB = null;
            converge(nodeA);
            Subscriber subscriber = subscribe(nodeA, new FindCourseCatalog("computer-science"));

            try {
                subscriber.awaitReceived(1);

                // when node B joins, handling the query too
                nodeB = startNode(NODE_B, portB);
                converge();

                // then the updates node B emitted before it was subscribed to are lost, so carrying on would be
                // silently incomplete
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> subscriber.failure.get() != null);
                assertThat(subscriber.failure.get()).isInstanceOf(SubscriptionQueryMembersChangedException.class);
            } finally {
                subscriber.dispose();
            }
        }

        @Test
        void carriesOnWithTheRemainingNodeWhenAHandlingNodeShutsDown() {
            // given a subscription to a query both nodes handle
            Subscriber subscriber = subscribe(nodeA, new FindCourseCatalog("computer-science"));

            try {
                subscriber.awaitReceived(1);

                // when node B shuts down cleanly while answering the subscription
                long shutdownStarted = System.nanoTime();
                nodeB.close();
                Duration shutdown = Duration.ofNanos(System.nanoTime() - shutdownStarted);
                nodeB = null;
                emitUpdateOn(nodeA, FIND_COURSE_CATALOG, "catalog@" + NODE_A);

                // then the subscription goes on with the node still there, and node B's shutdown did not wait on it
                Awaitility.await().atMost(Duration.ofSeconds(20))
                          .until(() -> subscriber.received.size() >= 2 || subscriber.failure.get() != null);
                assertThat(subscriber.failure.get()).as("failure after a shutdown taking %s", shutdown).isNull();
                assertThat(subscriber.received.get(1)).isEqualTo("catalog@" + NODE_A);
                assertThat(subscriber.completed).isFalse();
                assertThat(shutdown).isLessThan(Duration.ofSeconds(10));
            } finally {
                subscriber.dispose();
            }
        }

        private Subscriber subscribe(ConfigurableApplicationContext node, Object query) {
            Subscriber subscriber = new Subscriber();
            subscriber.subscription =
                    Flux.from(node.getBean(QueryGateway.class).subscriptionQuery(query, String.class))
                        .subscribe(subscriber.received::add,
                                   subscriber.failure::set,
                                   () -> subscriber.completed.set(true));
            return subscriber;
        }

        /**
         * Emits an update for every open subscription to the given query, on the given {@code node}.
         */
        private void emitUpdateOn(ConfigurableApplicationContext node, QualifiedName queryName, String update) {
            node.getBean(QueryBus.class)
                .emitUpdate(query -> queryName.equals(query.type().qualifiedName()),
                            () -> new GenericSubscriptionQueryUpdateMessage(new MessageType(String.class), update),
                            null)
                .orTimeout(20, TimeUnit.SECONDS)
                .join();
        }

        /**
         * What a subscriber to a subscription query has received, and how its subscription ended.
         */
        private static final class Subscriber {

            private final List<String> received = new CopyOnWriteArrayList<>();
            private final AtomicReference<Throwable> failure = new AtomicReference<>();
            private final AtomicBoolean completed = new AtomicBoolean();
            private Disposable subscription;

            private void awaitReceived(int count) {
                Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> received.size() >= count);
            }

            private void dispose() {
                subscription.dispose();
            }
        }
    }

    @Nested
    class ConvergingOnMembershipChanges {

        @Test
        void routesEverythingToTheRemainingNodeAfterOneLeaves() {
            // given both nodes are taking a share
            assertThat(Set.copyOf(handleCoursesFrom(nodeA, COMMAND_COUNT)))
                    .containsExactlyInAnyOrder(NODE_A, NODE_B);

            // when
            nodeB.close();
            nodeB = null;
            converge(nodeA);

            // then
            assertThat(Set.copyOf(handleCoursesFrom(nodeA, COMMAND_COUNT))).containsExactly(NODE_A);
        }

        @Test
        void takesUpTheShareOfANodeThatJoins() {
            // given only one node is running
            nodeB.close();
            nodeB = null;
            converge(nodeA);
            assertThat(Set.copyOf(handleCoursesFrom(nodeA, COMMAND_COUNT))).containsExactly(NODE_A);

            // when
            nodeB = startNode(NODE_B, portB);
            converge();

            // then
            assertThat(Set.copyOf(handleCoursesFrom(nodeA, COMMAND_COUNT)))
                    .containsExactlyInAnyOrder(NODE_A, NODE_B);
        }
    }

    @Nested
    class ServingCapabilities {

        @Test
        void reportsTheCommandsANodeHandles() {
            // when
            MemberCapabilitiesPayload payload = capabilitiesOf(portB).getBody();

            // then
            assertThat(payload).isNotNull();
            assertThat(payload.commands())
                    .contains(CreateCourse.class.getPackageName() + ".CreateCourse");
            assertThat(payload.loadFactor()).isPositive();
        }

        @Test
        void reportsTheQueriesANodeHandles() {
            // when
            MemberCapabilitiesPayload payload = capabilitiesOf(portB).getBody();

            // then
            assertThat(payload).isNotNull();
            assertThat(payload.queries())
                    .contains(FindCourse.class.getPackageName() + ".FindCourse");
        }

        @Test
        void answersNotModifiedWhenTheCapabilitiesAreUnchanged() {
            // given
            String entityTag = capabilitiesOf(portB).getHeaders().getETag();
            assertThat(entityTag).isNotNull();

            // when
            ResponseEntity<MemberCapabilitiesPayload> conditional =
                    restClient.get()
                              .uri(capabilitiesUri(portB))
                              .header("If-None-Match", entityTag)
                              .retrieve()
                              .toEntity(MemberCapabilitiesPayload.class);

            // then a steady-state poll costs a round trip and nothing more
            assertThat(conditional.getStatusCode()).isEqualTo(HttpStatus.NOT_MODIFIED);
            assertThat(conditional.getBody()).isNull();
        }

        private ResponseEntity<MemberCapabilitiesPayload> capabilitiesOf(int port) {
            return restClient.get()
                             .uri(capabilitiesUri(port))
                             .retrieve()
                             .toEntity(MemberCapabilitiesPayload.class);
        }

        private String capabilitiesUri(int port) {
            return "http://localhost:" + port + RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_ENDPOINT;
        }
    }
}
