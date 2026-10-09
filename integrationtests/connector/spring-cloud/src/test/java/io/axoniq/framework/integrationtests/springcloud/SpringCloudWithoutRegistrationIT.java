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
import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberDiscovery;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A two-node test of the Spring Cloud connector on a discovery implementation that, like Spring Cloud Kubernetes,
 * supplies a {@link org.springframework.cloud.client.discovery.DiscoveryClient} but no {@link Registration}, and
 * publishes a {@link HeartbeatEvent} only when the instances it reports change.
 * <p>
 * Neither node is given a registration. The nodes find each other, and themselves, through the node id each serves on
 * its capabilities endpoint. Discovery reports every node a second time, under another service id and address, as it
 * would an application selected by two Kubernetes Services. Each node gets one heartbeat, as the endpoints change when
 * it starts; node A gets its heartbeat before node B answers, as a pod can be listed before it is serving, and only
 * learns about node B from refreshing capabilities.
 *
 * @author Allard Buijze
 */
class SpringCloudWithoutRegistrationIT {

    private static final String NODE_A = "node-a";
    private static final String NODE_B = "node-b";

    private int portA;
    private int portB;
    private ConfigurableApplicationContext nodeA;
    private ConfigurableApplicationContext nodeB;

    @BeforeEach
    void setUp() throws IOException {
        portA = freePort();
        portB = freePort();
        nodeA = startNode(NODE_A, portA);
        nodeA.publishEvent(new HeartbeatEvent(this, "node-a-started"));
        nodeB = startNode(NODE_B, portB);
        nodeB.publishEvent(new HeartbeatEvent(this, "node-b-started"));
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
                            "test.cluster.alias-service=courses-svc",
                            "test.registration.enabled=false",
                            "axon.springcloud.capabilities-refresh-interval=500ms",
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

    @Test
    void startsWithoutARegistration() {
        // then
        assertThat(nodeA.getBeansOfType(Registration.class)).isEmpty();
        assertThat(nodeA.getBean(SpringCloudMemberDiscovery.class).localMember().name())
                .isEqualTo(nodeA.getBean(CapabilityDiscoveryMode.class).localNodeId());
    }

    @Test
    void findsEachNodeOnceWithoutFurtherHeartbeats() {
        // then — four instances are reported, but they are two members, each recognizing itself as the local one
        Awaitility.await()
                  .atMost(Duration.ofSeconds(20))
                  .untilAsserted(() -> {
                      for (ConfigurableApplicationContext node : List.of(nodeA, nodeB)) {
                          SpringCloudMemberDiscovery discovery = node.getBean(SpringCloudMemberDiscovery.class);
                          Set<Member> members = discovery.members();
                          assertThat(members).hasSize(2);
                          assertThat(members).filteredOn(Member::local).containsExactly(discovery.localMember());
                      }
                  });
    }

    @Test
    void placesEveryMemberTheSameWayOnEveryNode() {
        // then — both nodes know the same members by the same names, so they agree on where a routing key belongs
        Awaitility.await()
                  .atMost(Duration.ofSeconds(20))
                  .untilAsserted(() -> assertThat(memberNamesOf(nodeA)).hasSize(2)
                                                                     .isEqualTo(memberNamesOf(nodeB)));
    }

    @Test
    void distributesCommandsAcrossBothNodes() {
        // given
        CommandGateway gateway = nodeA.getBean(CommandGateway.class);

        // when / then — commands reach both nodes once node A has refreshed what node B handles
        Awaitility.await()
                  .atMost(Duration.ofSeconds(20))
                  .untilAsserted(() -> {
                      List<String> handledBy = new ArrayList<>();
                      for (int i = 0; i < 40; i++) {
                          handledBy.add(gateway.sendAndWait(new CreateCourse("course-" + i, "Axon 5"),
                                                            String.class));
                      }
                      assertThat(Set.copyOf(handledBy)).containsExactlyInAnyOrder(NODE_A, NODE_B);
                  });
    }

    private static Set<String> memberNamesOf(ConfigurableApplicationContext node) {
        return node.getBean(SpringCloudMemberDiscovery.class)
                   .members()
                   .stream()
                   .map(Member::name)
                   .collect(Collectors.toSet());
    }
}
