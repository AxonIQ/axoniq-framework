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

package io.axoniq.framework.axonserver.connector;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ReconnectConfiguration;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.AxonServerException;
import io.axoniq.framework.axonserver.connector.api.TagsConfiguration;
import io.axoniq.framework.axonserver.connector.event.StubServer;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.axoniq.framework.axonserver.connector.util.TcpUtils;
import io.axoniq.framework.axonserver.connector.util.PlatformService;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.ReflectionUtils;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.axoniq.framework.axonserver.connector.util.AssertUtils.assertWithin;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link AxonServerConnectionManager}.
 *
 * @author Milan Savic
 */
class AxonServerConnectionManagerTest {

    private static final String TEST_CONTEXT = "default";

    private StubServer stubServer;
    private StubServer secondNode;
    private AxonServerConfiguration testConfig;

    @BeforeEach
    void setUp() throws IOException {
        int port1 = TcpUtils.findFreePort();
        int port2 = TcpUtils.findFreePort();
        stubServer = new StubServer(port1, port2);
        secondNode = new StubServer(port2, port2);
        stubServer.start();
        secondNode.start();
        testConfig = AxonServerConfiguration.builder()
                                            .context(TEST_CONTEXT)
                                            .servers("localhost:" + stubServer.getPort())
                                            .build();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        stubServer.shutdown();
        secondNode.shutdown();
    }

    @Test
    void whetherConnectionPreferenceIsSent() {
        TagsConfiguration testTags = new TagsConfiguration(Collections.singletonMap("key", "value"));

        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .tagsConfiguration(testTags)
                                                                             .build();

        assertThat(testSubject.getConnection(TEST_CONTEXT)).isNotNull();

        List<ClientIdentification> clientIdentificationRequests = stubServer.getPlatformService()
                                                                            .getClientIdentificationRequests();
        assertThat(clientIdentificationRequests).hasSize(1);
        Map<String, String> expectedTags = clientIdentificationRequests.get(0).getTagsMap();
        assertThat(expectedTags)
                .isNotNull()
                .hasSize(1);
        assertThat(expectedTags).containsEntry("key", "value");

        assertWithin(
                1, TimeUnit.SECONDS,
                () -> assertThat(secondNode.getPlatformService().getClientIdentificationRequests()).hasSize(1)
        );

        List<ClientIdentification> clients = secondNode.getPlatformService().getClientIdentificationRequests();
        Map<String, String> connectionExpectedTags = clients.getFirst().getTagsMap();
        assertThat(connectionExpectedTags)
                .isNotNull()
                .hasSize(1);
        assertThat(connectionExpectedTags).containsEntry("key", "value");
    }

    @Test
    void connectionTimeout() throws IOException, InterruptedException {
        stubServer.shutdown();
        stubServer = new StubServer(TcpUtils.findFreePort(), new PlatformService(TcpUtils.findFreePort()) {
            @Override
            public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
                // ignore calls
            }
        });
        stubServer.start();

        AxonServerConfiguration testConfig = AxonServerConfiguration.builder()
                                                                    .servers("localhost:" + stubServer.getPort())
                                                                    .connectTimeout(50)
                                                                    .build();

        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();

        try {
            AxonServerConnection connection = testSubject.getConnection();
            connection.commandChannel();
            assertWithin(
                    2, TimeUnit.SECONDS,
                    () -> assertThat(connection.isConnectionFailed()).as("Was not expecting to get a connection").isTrue()
            );
        } catch (AxonServerException e) {
            assertThat(e.getMessage()).contains("connection");
        }
    }

    @Test
    void enablingHeartbeatsEnsuresHeartbeatMessagesAreSent() {
        testConfig.getHeartbeat().setEnabled(true);
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();
        testSubject.start();

        assertThat(testSubject.getConnection(testConfig.getContext())).isNotNull();

        assertWithin(
                250, TimeUnit.MILLISECONDS,
                // Retrieving the messages from the secondNode, as the stubServer forwards all messages to this instance
                () -> assertThat(secondNode.getPlatformService().getHeartbeatMessages(testConfig.getContext())).hasSize(1)
        );
    }

    @Test
    void enablingHeartbeatsEnsuresHeartbeatMessagesAreSentOnOtherContexts() {
        testConfig.getHeartbeat().setEnabled(true);
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();
        testSubject.start();

        assertThat(testSubject.getConnection(testConfig.getContext())).isNotNull();
        assertThat(testSubject.getConnection("context2")).isNotNull();

        assertWithin(
                250, TimeUnit.MILLISECONDS,
                // Retrieving the messages from the secondNode, as the stubServer forwards all messages to this instance
                () -> {
                    assertThat(secondNode.getPlatformService().getHeartbeatMessages(testConfig.getContext())).isNotEmpty();
                    assertThat(secondNode.getPlatformService().getHeartbeatMessages("context2")).isNotEmpty();
                }
        );
    }

    @Test
    void disablingHeartbeatsEnsuresNoHeartbeatMessagesAreSent() {
        testConfig.getHeartbeat().setEnabled(false);
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();
        testSubject.start();

        assertThat(testSubject.getConnection(testConfig.getContext())).isNotNull();

        assertWithin(
                250, TimeUnit.MILLISECONDS,
                // Retrieving the messages from the secondNode, as the stubServer forwards all messages to this instance
                () -> assertThat(secondNode.getPlatformService().getHeartbeatMessages()).isEmpty()
        );
    }

    @Test
    void channelCustomization() {
        AtomicBoolean interceptorCalled = new AtomicBoolean();
        AxonServerConnectionManager testSubject =
                AxonServerConnectionManager.builder()
                                           .axonServerConfiguration(testConfig)
                                           .channelCustomizer(
                                                   builder -> builder.intercept(new ClientInterceptor() {
                                                       @Override
                                                       public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                                               MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                               CallOptions callOptions,
                                                               Channel channel
                                                       ) {
                                                           interceptorCalled.set(true);
                                                           return channel.newCall(methodDescriptor, callOptions);
                                                       }
                                                   })
                                           )
                                           .build();

        assertThat(testSubject.getConnection()).isNotNull();
        assertThat(interceptorCalled.get()).isTrue();
    }

    @Test
    void isConnected() {
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();

        // Creates a connection for the default context
        AxonServerConnection result = testSubject.getConnection();
        assertWithin(250, TimeUnit.MILLISECONDS, () -> assertThat(result.isReady()).isTrue());

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isTrue();
        assertThat(testSubject.isConnected("unknown-context")).isFalse();
    }

    @Test
    void shutdownClosesAllConnections() {
        AxonServerConnectionManager testSubject =
                AxonServerConnectionManager.builder()
                                           .axonServerConfiguration(testConfig)
                                           .build();

        // Creates several connections
        AxonServerConnection channelOne = testSubject.getConnection(TEST_CONTEXT);
        AxonServerConnection channelTwo = testSubject.getConnection("some-other-context");
        assertWithin(250, TimeUnit.MILLISECONDS, () -> {
            assertThat(channelOne.isReady()).isTrue();
            assertThat(channelTwo.isReady()).isTrue();
        });

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isTrue();
        assertThat(testSubject.isConnected("some-other-context")).isTrue();

        // Shutdown the connection manager
        testSubject.shutdown();

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isFalse();
        assertThat(testSubject.isConnected("some-other-context")).isFalse();
        assertThat(secondNode.getPlatformService().getNumberOfCompletedStreams()).isEqualTo(2);
    }

    @Test
    void disconnectClosesAllConnections() {
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();

        // Creates several connections
        AxonServerConnection channelOne = testSubject.getConnection(TEST_CONTEXT);
        AxonServerConnection channelTwo = testSubject.getConnection("some-other-context");
        assertWithin(250, TimeUnit.MILLISECONDS, () -> {
            assertThat(channelOne.isReady()).isTrue();
            assertThat(channelTwo.isReady()).isTrue();
        });

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isTrue();
        assertThat(testSubject.isConnected("some-other-context")).isTrue();

        // Close all connections
        testSubject.disconnect();

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isFalse();
        assertThat(testSubject.isConnected("some-other-context")).isFalse();
        assertThat(secondNode.getPlatformService().getNumberOfCompletedStreams()).isEqualTo(2);
    }

    @Test
    void disconnectSingleConnection() {
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();

        // Creates a connection for the default context
        AxonServerConnection channelOne = testSubject.getConnection(TEST_CONTEXT);
        AxonServerConnection channelTwo = testSubject.getConnection("some-other-context");
        assertWithin(250, TimeUnit.MILLISECONDS, () -> {
            assertThat(channelOne.isReady()).isTrue();
            assertThat(channelTwo.isReady()).isTrue();
        });

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isTrue();
        assertThat(testSubject.isConnected("some-other-context")).isTrue();

        // Will close the default connection only
        testSubject.disconnect(TEST_CONTEXT);

        assertThat(testSubject.isConnected(TEST_CONTEXT)).isFalse();
        assertThat(testSubject.isConnected("some-other-context")).isTrue();
        assertThat(secondNode.getPlatformService().getNumberOfCompletedStreams()).isEqualTo(1);
    }

    @Test
    void connectionsReturnsConnectionStatus() {
        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();

        AxonServerConnection channelOne = testSubject.getConnection(TEST_CONTEXT);
        AxonServerConnection channelTwo = testSubject.getConnection("some-other-context");

        assertWithin(250, TimeUnit.MILLISECONDS, () -> {
            assertThat(channelOne.isReady()).isTrue();
            assertThat(channelTwo.isReady()).isTrue();
        });

        Map<String, Boolean> results = testSubject.connections();

        Boolean testContextConnection = results.get(TEST_CONTEXT);
        assertThat(testContextConnection)
                .isNotNull()
                .isTrue();

        Boolean someOtherContextConnection = results.get("some-other-context");
        assertThat(someOtherContextConnection)
                .isNotNull()
                .isTrue();
    }

    @Test
    void axonServerConfigurationIsSetOnAxonServerConnectionFactoryAsExpected() throws NoSuchFieldException {
        long expectedReconnectInterval = 1337L;

        AxonServerConfiguration testConfig = new AxonServerConfiguration();
        testConfig.setReconnectInterval(expectedReconnectInterval);
        testConfig.setForceReconnectThroughServers(false);

        AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                             .axonServerConfiguration(testConfig)
                                                                             .build();

        AxonServerConnectionFactory connectionFactory = ReflectionUtils.getFieldValue(
                AxonServerConnectionManager.class.getDeclaredField("connectionFactory"), testSubject
        );

        ReconnectConfiguration resultReconnectConfig = ReflectionUtils.getFieldValue(
                AxonServerConnectionFactory.class.getDeclaredField("reconnectConfiguration"), connectionFactory
        );
        assertThat(resultReconnectConfig.getReconnectInterval()).isEqualTo(expectedReconnectInterval);
        assertThat(resultReconnectConfig.isForcePlatformReconnect()).isFalse();
    }

    @Test
    void buildWithNullAxonServerConfigurationThrowsAxonConfigurationException() {
        AxonServerConnectionManager.Builder builderTestSubject = AxonServerConnectionManager.builder();
        assertThatThrownBy(() -> builderTestSubject.axonServerConfiguration(null))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNullTagsConfigurationThrowsAxonConfigurationException() {
        AxonServerConnectionManager.Builder builderTestSubject = AxonServerConnectionManager.builder();
        assertThatThrownBy(() -> builderTestSubject.tagsConfiguration(null))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithoutAxonServerConfigurationThrowsAxonConfigurationException() {
        AxonServerConnectionManager.Builder builderTestSubject = AxonServerConnectionManager.builder();
        assertThatThrownBy(builderTestSubject::build)
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void buildWithNullRoutingServersThrowsAxonConfigurationException() {
        AxonServerConnectionManager.Builder builderTestSubject = AxonServerConnectionManager.builder();
        assertThatThrownBy(() -> builderTestSubject.routingServers(null))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Nested
    class MutualTlsConfiguration {

        @Test
        void buildWithMutualTlsConfigurationSucceeds() {
            AxonServerConfiguration config = AxonServerConfiguration.builder()
                                                                    .servers("localhost:" + stubServer.getPort())
                                                                    .mutualTls(tlsFile("client.crt"),
                                                                               tlsFile("client.key"))
                                                                    .build();
            config.setCertFile(tlsFile("ca.crt"));

            AxonServerConnectionManager testSubject = AxonServerConnectionManager.builder()
                                                                                 .axonServerConfiguration(config)
                                                                                 .build();

            assertThat(testSubject).isNotNull();
        }

        @Test
        void buildWithClientCertificateWithoutClientKeyThrowsAxonConfigurationException() {
            AxonServerConfiguration config = AxonServerConfiguration.builder()
                                                                    .servers("localhost:" + stubServer.getPort())
                                                                    .ssl(tlsFile("ca.crt"))
                                                                    .build();
            config.setClientCertFile(tlsFile("client.crt"));

            AxonServerConnectionManager.Builder builderTestSubject =
                    AxonServerConnectionManager.builder().axonServerConfiguration(config);
            assertThatThrownBy(builderTestSubject::build)
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("client key file is missing");
        }

        @Test
        void buildWithClientKeyWithoutClientCertificateThrowsAxonConfigurationException() {
            AxonServerConfiguration config = AxonServerConfiguration.builder()
                                                                    .servers("localhost:" + stubServer.getPort())
                                                                    .ssl(tlsFile("ca.crt"))
                                                                    .build();
            config.setClientKeyFile(tlsFile("client.key"));

            AxonServerConnectionManager.Builder builderTestSubject =
                    AxonServerConnectionManager.builder().axonServerConfiguration(config);
            assertThatThrownBy(builderTestSubject::build)
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("client certificate file is missing");
        }

        @Test
        void buildWithClientCertificateWithoutSslEnabledThrowsAxonConfigurationException() {
            AxonServerConfiguration config = AxonServerConfiguration.builder()
                                                                    .servers("localhost:" + stubServer.getPort())
                                                                    .build();
            config.setClientCertFile(tlsFile("client.crt"));
            config.setClientKeyFile(tlsFile("client.key"));

            AxonServerConnectionManager.Builder builderTestSubject =
                    AxonServerConnectionManager.builder().axonServerConfiguration(config);
            assertThatThrownBy(builderTestSubject::build)
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("SSL is not enabled");
        }

        private String tlsFile(String name) {
            try {
                return new File(getClass().getResource("/tls/" + name).toURI()).getAbsolutePath();
            } catch (Exception e) {
                throw new IllegalStateException("Could not locate test resource /tls/" + name, e);
            }
        }
    }

    @Test
    void buildWithEmptyRoutingServersThrowsAxonConfigurationException() {
        AxonServerConnectionManager.Builder builderTestSubject = AxonServerConnectionManager.builder();
        assertThatThrownBy(() -> builderTestSubject.routingServers(""))
                .isInstanceOf(AxonConfigurationException.class);
    }
}
