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

import org.axonframework.messaging.commandhandling.annotation.Command;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The application and command each node of the two-node Spring Cloud test runs.
 * <p>
 * Discovery is supplied directly rather than through a discovery implementation such as Eureka. What is under test is
 * the connector's routing and transport, and standing up a registry would add a moving part without exercising a line
 * of it: both nodes' addresses are known to the test, so it can report them itself.
 *
 * @author Allard Buijze
 */
final class SpringCloudNodes {

    /**
     * The service id both nodes register under, and so the cluster they form.
     */
    static final String SERVICE_ID = "university";

    private SpringCloudNodes() {
        // Utility class
    }

    /**
     * A command routed by its {@code courseId}, so that every command for one course reaches one node.
     *
     * @param courseId the course the command concerns, and the key it is routed by
     * @param name     the name to give the course
     */
    @Command(routingKey = "courseId")
    record CreateCourse(String courseId, String name) {

    }

    /**
     * A query used by the test to observe query routing independently of command routing.
     *
     * @param courseId the course to find
     */
    record FindCourse(String courseId) {

    }

    /**
     * The application each node runs.
     * <p>
     * Both nodes run the same application and handle the same command; which node handles a given command is decided
     * entirely by the routing ring.
     */
    @SpringBootApplication
    static class NodeApplication {

        /**
         * Reports both nodes of the cluster to the connector, standing in for a discovery registry.
         *
         * @param nodePorts the ports of every node in the cluster, comma-separated
         * @return a discovery client reporting every node in the cluster
         */
        @Bean
        DiscoveryClient discoveryClient(@Value("${test.cluster.ports}") String nodePorts) {
            List<ServiceInstance> instances =
                    Arrays.stream(nodePorts.split(","))
                                    .map(String::trim)
                                    .map(port -> (ServiceInstance) new DefaultServiceInstance(
                                            SERVICE_ID + "-" + port, SERVICE_ID, "localhost",
                                            Integer.parseInt(port), false, Map.of()
                                    ))
                                    .toList();
            return new ClusterDiscoveryClient(instances);
        }

        /**
         * Identifies this node within the cluster, which is how the connector tells its own instance from the others.
         *
         * @param port the port this node serves on
         * @return the registration representing this node
         */
        @Bean
        Registration registration(@Value("${server.port}") int port) {
            return new NodeRegistration(new DefaultServiceInstance(
                    SERVICE_ID + "-" + port, SERVICE_ID, "localhost", port, false, Map.of()
            ));
        }

    }

    /**
     * Handles {@link CreateCourse} on whichever node the command was routed to, answering with that node's name.
     * <p>
     * Returning the node's own name is what lets the test see where a command was handled, without any shared state
     * between the two application contexts.
     */
    @Component
    static class CreateCourseHandler {

        private final String nodeName;

        CreateCourseHandler(@Value("${test.node.name}") String nodeName) {
            this.nodeName = nodeName;
        }

        @CommandHandler
        String handle(CreateCourse command) {
            return nodeName;
        }
    }

    /**
     * Handles {@link FindCourse} on the node where this test enables the handler, answering with that node's name.
     */
    @Component
    @ConditionalOnProperty(name = "test.query-handler.enabled", havingValue = "true")
    static class FindCourseHandler {

        private final String nodeName;

        FindCourseHandler(@Value("${test.node.name}") String nodeName) {
            this.nodeName = nodeName;
        }

        @QueryHandler
        String handle(FindCourse query) {
            return nodeName;
        }
    }

    /**
     * Reports a fixed set of instances as the cluster.
     */
    private record ClusterDiscoveryClient(List<ServiceInstance> instances) implements DiscoveryClient {

        @Override
        public String description() {
            return "Two-node test cluster";
        }

        @Override
        public List<ServiceInstance> getInstances(String serviceId) {
            return SERVICE_ID.equals(serviceId) ? instances : List.of();
        }

        @Override
        public List<String> getServices() {
            return List.of(SERVICE_ID);
        }
    }

    /**
     * A {@link Registration} delegating to a {@link ServiceInstance}, since {@code Registration} adds nothing to it.
     */
    private record NodeRegistration(ServiceInstance delegate) implements Registration {

        @Override
        public String getServiceId() {
            return delegate.getServiceId();
        }

        @Override
        public String getHost() {
            return delegate.getHost();
        }

        @Override
        public int getPort() {
            return delegate.getPort();
        }

        @Override
        public boolean isSecure() {
            return delegate.isSecure();
        }

        @Override
        public URI getUri() {
            return delegate.getUri();
        }

        @Override
        public Map<String, String> getMetadata() {
            return delegate.getMetadata();
        }
    }
}
