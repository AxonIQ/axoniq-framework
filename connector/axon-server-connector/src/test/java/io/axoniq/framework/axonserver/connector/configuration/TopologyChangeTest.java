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

package io.axoniq.framework.axonserver.connector.configuration;

import io.axoniq.axonserver.grpc.control.CommandSubscription;
import io.axoniq.axonserver.grpc.control.QuerySubscription;
import io.axoniq.axonserver.grpc.control.UpdateType;
import org.junit.jupiter.api.*;

import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link TopologyChange}.
 *
 * @author Steven van Beelen
 */
class TopologyChangeTest {

    private static final String TEST_CONTEXT = "context";
    private static final String TEST_CLIENT_ID = "clientId";
    private static final String TEST_CLIENT_STREAM_ID = "clientStreamId";
    private static final String TEST_COMPONENT_NAME = "componentName";
    private static final String TEST_COMMAND_NAME = "commandName";
    private static final int TEST_LOAD_FACTOR = 42;
    private static final String TEST_QUERY_NAME = "queryName";

    @Test
    void mapsGrpcBasedAddCommandChange() {
        CommandSubscription commandSubscription = CommandSubscription.newBuilder()
                                                                     .setName(TEST_COMMAND_NAME)
                                                                     .setLoadFactor(TEST_LOAD_FACTOR)
                                                                     .build();
        io.axoniq.axonserver.grpc.control.TopologyChange grpcBasedChange =
                io.axoniq.axonserver.grpc.control.TopologyChange.newBuilder()
                                                                .setUpdateType(UpdateType.ADD_COMMAND_HANDLER)
                                                                .setContext(TEST_CONTEXT)
                                                                .setClientId(TEST_CLIENT_ID)
                                                                .setClientStreamId(TEST_CLIENT_STREAM_ID)
                                                                .setComponentName(TEST_COMPONENT_NAME)
                                                                .setCommand(commandSubscription)
                                                                .build();

        TopologyChange testSubject = new TopologyChange(grpcBasedChange);

        assertThat(testSubject.type()).isEqualTo(TopologyChange.Type.COMMAND_HANDLER_ADDED);
        assertThat(testSubject.context()).isEqualTo(TEST_CONTEXT);
        assertThat(testSubject.clientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(testSubject.clientStreamId()).isEqualTo(TEST_CLIENT_STREAM_ID);
        assertThat(testSubject.componentName()).isEqualTo(TEST_COMPONENT_NAME);
        TopologyChange.HandlerSubscription handlerSubscription = testSubject.handler();
        assertThat(handlerSubscription).isNotNull();
        assertThat(handlerSubscription.name()).isEqualTo(TEST_COMMAND_NAME);
        OptionalInt optionalLoadFactor = handlerSubscription.loadFactor();
        assertThat(optionalLoadFactor).isPresent();
        assertThat(optionalLoadFactor.getAsInt()).isEqualTo(TEST_LOAD_FACTOR);
    }

    @Test
    void mapsGrpcBasedRemoveCommandChange() {
        CommandSubscription commandSubscription = CommandSubscription.newBuilder()
                                                                     .setName(TEST_COMMAND_NAME)
                                                                     .setLoadFactor(TEST_LOAD_FACTOR)
                                                                     .build();
        io.axoniq.axonserver.grpc.control.TopologyChange grpcBasedChange =
                io.axoniq.axonserver.grpc.control.TopologyChange.newBuilder()
                                                                .setUpdateType(UpdateType.REMOVE_COMMAND_HANDLER)
                                                                .setContext(TEST_CONTEXT)
                                                                .setClientId(TEST_CLIENT_ID)
                                                                .setClientStreamId(TEST_CLIENT_STREAM_ID)
                                                                .setComponentName(TEST_COMPONENT_NAME)
                                                                .setCommand(commandSubscription)
                                                                .build();

        TopologyChange testSubject = new TopologyChange(grpcBasedChange);

        assertThat(testSubject.type()).isEqualTo(TopologyChange.Type.COMMAND_HANDLER_REMOVED);
        assertThat(testSubject.context()).isEqualTo(TEST_CONTEXT);
        assertThat(testSubject.clientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(testSubject.clientStreamId()).isEqualTo(TEST_CLIENT_STREAM_ID);
        assertThat(testSubject.componentName()).isEqualTo(TEST_COMPONENT_NAME);
        TopologyChange.HandlerSubscription handlerSubscription = testSubject.handler();
        assertThat(handlerSubscription).isNotNull();
        assertThat(handlerSubscription.name()).isEqualTo(TEST_COMMAND_NAME);
        OptionalInt optionalLoadFactor = handlerSubscription.loadFactor();
        assertThat(optionalLoadFactor).isPresent();
        assertThat(optionalLoadFactor.getAsInt()).isEqualTo(TEST_LOAD_FACTOR);
    }

    @Test
    void mapsGrpcBasedAddQueryChange() {
        QuerySubscription querySubscription = QuerySubscription.newBuilder()
                                                               .setName(TEST_QUERY_NAME)
                                                               .build();
        io.axoniq.axonserver.grpc.control.TopologyChange grpcBasedChange =
                io.axoniq.axonserver.grpc.control.TopologyChange.newBuilder()
                                                                .setUpdateType(UpdateType.ADD_QUERY_HANDLER)
                                                                .setContext(TEST_CONTEXT)
                                                                .setClientId(TEST_CLIENT_ID)
                                                                .setClientStreamId(TEST_CLIENT_STREAM_ID)
                                                                .setComponentName(TEST_COMPONENT_NAME)
                                                                .setQuery(querySubscription)
                                                                .build();

        TopologyChange testSubject = new TopologyChange(grpcBasedChange);

        assertThat(testSubject.type()).isEqualTo(TopologyChange.Type.QUERY_HANDLER_ADDED);
        assertThat(testSubject.context()).isEqualTo(TEST_CONTEXT);
        assertThat(testSubject.clientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(testSubject.clientStreamId()).isEqualTo(TEST_CLIENT_STREAM_ID);
        assertThat(testSubject.componentName()).isEqualTo(TEST_COMPONENT_NAME);
        TopologyChange.HandlerSubscription handlerSubscription = testSubject.handler();
        assertThat(handlerSubscription).isNotNull();
        assertThat(handlerSubscription.name()).isEqualTo(TEST_QUERY_NAME);
        assertThat(handlerSubscription.loadFactor().isPresent()).isFalse();
    }

    @Test
    void mapsGrpcBasedRemoveQueryChange() {
        QuerySubscription querySubscription = QuerySubscription.newBuilder()
                                                               .setName(TEST_QUERY_NAME)
                                                               .build();
        io.axoniq.axonserver.grpc.control.TopologyChange grpcBasedChange =
                io.axoniq.axonserver.grpc.control.TopologyChange.newBuilder()
                                                                .setUpdateType(UpdateType.REMOVE_QUERY_HANDLER)
                                                                .setContext(TEST_CONTEXT)
                                                                .setClientId(TEST_CLIENT_ID)
                                                                .setClientStreamId(TEST_CLIENT_STREAM_ID)
                                                                .setComponentName(TEST_COMPONENT_NAME)
                                                                .setQuery(querySubscription)
                                                                .build();

        TopologyChange testSubject = new TopologyChange(grpcBasedChange);

        assertThat(testSubject.type()).isEqualTo(TopologyChange.Type.QUERY_HANDLER_REMOVED);
        assertThat(testSubject.context()).isEqualTo(TEST_CONTEXT);
        assertThat(testSubject.clientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(testSubject.clientStreamId()).isEqualTo(TEST_CLIENT_STREAM_ID);
        assertThat(testSubject.componentName()).isEqualTo(TEST_COMPONENT_NAME);
        TopologyChange.HandlerSubscription handlerSubscription = testSubject.handler();
        assertThat(handlerSubscription).isNotNull();
        assertThat(handlerSubscription.name()).isEqualTo(TEST_QUERY_NAME);
        assertThat(handlerSubscription.loadFactor().isPresent()).isFalse();
    }

    @Test
    void mapsGrpcBasedResetChange() {
        io.axoniq.axonserver.grpc.control.TopologyChange grpcBasedChange =
                io.axoniq.axonserver.grpc.control.TopologyChange.newBuilder()
                                                                .setUpdateType(UpdateType.RESET_ALL)
                                                                .setContext(TEST_CONTEXT)
                                                                .setClientId(TEST_CLIENT_ID)
                                                                .setClientStreamId(TEST_CLIENT_STREAM_ID)
                                                                .setComponentName(TEST_COMPONENT_NAME)
                                                                .build();


        TopologyChange testSubject = new TopologyChange(grpcBasedChange);

        assertThat(testSubject.type()).isEqualTo(TopologyChange.Type.RESET);
        assertThat(testSubject.context()).isEqualTo(TEST_CONTEXT);
        assertThat(testSubject.clientId()).isEqualTo(TEST_CLIENT_ID);
        assertThat(testSubject.clientStreamId()).isEqualTo(TEST_CLIENT_STREAM_ID);
        assertThat(testSubject.componentName()).isEqualTo(TEST_COMPONENT_NAME);
        assertThat(testSubject.handler()).isNull();
    }
}