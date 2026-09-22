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
import io.axoniq.framework.springcloud.SpringCloudMemberRegistry;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesPayload;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.routing.Member;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.junit.jupiter.api.*;
import org.awaitility.Awaitility;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.time.Duration;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A two-node test of the Spring Cloud connector, distributing commands over HTTP between two Spring Boot applications
 * in one JVM.
 * <p>
 * Both nodes handle the same command, so where a command is handled is decided by the routing ring alone. Each node's
 * handler answers with its own name, which is how the test observes routing without reaching into either node.
 *
 * @author Allard Buijze
 */
class SpringCloudCommandDistributionIT {

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
        // addresses up front — there is no registry here to discover them from.
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
                            // Distributing through Spring Cloud and through Axon Server are alternatives, and this
                            // module carries the Axon Server connector on its classpath, so it is switched off here.
                            // A real Spring Cloud deployment would not have it at all.
                            "axon.axonserver.enabled=false",
                            "axon.multitenancy.enabled=false",
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
        SpringCloudMemberRegistry registry = node.getBean(SpringCloudMemberRegistry.class);
        return registry.ring().members().stream()
                       .filter(member -> registry.ring().capabilitiesOf(member)
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

    @Nested
    class DistributingCommands {

        @Test
        void distributesCommandsAcrossBothNodes() {
            // when
            List<String> handledBy = handleCoursesFrom(nodeA, COMMAND_COUNT);

            // then — commands leave the node they were dispatched from, which is the whole point of the connector
            assertThat(Set.copyOf(handledBy)).containsExactlyInAnyOrder(NODE_A, NODE_B);
        }

        @Test
        void routesTheSameCourseToTheSameNode() {
            // given
            CommandGateway gateway = nodeA.getBean(CommandGateway.class);

            // when — the same routing key, dispatched repeatedly
            Set<String> handlers = IntStream.range(0, 20)
                                            .mapToObj(i -> gateway.<String>sendAndWait(
                                                    new CreateCourse("course-7", "Axon 5"), String.class
                                            ))
                                            .collect(Collectors.toSet());

            // then — one course is handled on one node, which is what keeps its commands serialized
            assertThat(handlers).hasSize(1);
        }

        @Test
        void routesTheSameCourseToTheSameNodeFromEitherNode() {
            // when — the same routing key, dispatched from each node in turn
            CommandGateway fromA = nodeA.getBean(CommandGateway.class);
            CommandGateway fromB = nodeB.getBean(CommandGateway.class);
            String viaA = fromA.sendAndWait(new CreateCourse("course-7", "Axon 5"), String.class);
            String viaB = fromB.sendAndWait(new CreateCourse("course-7", "Axon 5"), String.class);

            // then — if the two nodes disagreed on where a key belongs, one course would be handled in two places
            assertThat(viaA).isEqualTo(viaB);
        }
    }

    @Nested
    class ConvergingOnMembershipChanges {

        @Test
        void routesEverythingToTheRemainingNodeAfterOneLeaves() {
            // given — both nodes are taking a share
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
            // given — only one node is running
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
        void reportsAnEmptyQuerySet() {
            // when
            MemberCapabilitiesPayload payload = capabilitiesOf(portB).getBody();

            // then — the field is served from the first release so the endpoint shape does not change when
            // query distribution arrives
            assertThat(payload).isNotNull();
            assertThat(payload.queries()).isEmpty();
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

            // then — a steady-state poll costs a round trip and nothing more
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
