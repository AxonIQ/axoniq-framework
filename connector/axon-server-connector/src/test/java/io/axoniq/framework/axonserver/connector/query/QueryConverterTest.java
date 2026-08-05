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

package io.axoniq.framework.axonserver.connector.query;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.framework.axonserver.connector.shared.ErrorCode;
import io.axoniq.framework.axonserver.connector.shared.ExceptionConverter;
import org.axonframework.common.AxonException;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryExecutionException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.junit.jupiter.*;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.axoniq.axonserver.grpc.ProcessingKey.*;
import static io.axoniq.axonserver.grpc.query.QueryResponse.newBuilder;
import static io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils.createProcessingInstruction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryConverterTest {

    private final String messageIdentifier = UUID.randomUUID().toString();
    private final byte[] payload = "payload".getBytes();
    private final String clientId = "clientId";
    private final String componentName = "componentName";
    private Converter converter;

    @BeforeEach
    void setUp() {
        converter = mock(Converter.class);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(converter);
    }

    @Test
    void convertsQueryRequestToQueryMessage() {
        String exObjectPayload = "exObjectPayload";
        var queryRequest = QueryRequest
                .newBuilder()
                .setMessageIdentifier(messageIdentifier)
                .setPayload(SerializedObject.newBuilder()
                                            .setType("QueryType")
                                            .setRevision("1")
                                            .setData(ByteString.copyFrom(payload))
                                            .build())
                .addProcessingInstructions(createProcessingInstruction(PRIORITY, 5))
                .build();

        when(converter.convert(any(), eq((Type)String.class)))
                .thenReturn(exObjectPayload);

        var queryMessage = QueryConverter.convertQueryRequest(queryRequest, converter);

        assertThat(queryMessage).isNotNull();
        assertThat(queryMessage.identifier()).isEqualTo(messageIdentifier);
        assertThat(queryMessage.type().name()).isEqualTo("QueryType");
        assertThat(queryMessage.payloadAs(byte[].class)).isEqualTo(payload);
        assertThat(queryMessage.priority()).hasValue(5);
        assertThat(queryMessage.payloadAs(String.class)).isEqualTo(exObjectPayload);

        verify(converter).convert(queryMessage.payload(), (Type) String.class);
    }

    @Test
    void convertsQueryMessageToQueryRequest() {
        var type = new MessageType("QueryType", "1");
        var qm = new GenericQueryMessage(
                new GenericMessage(messageIdentifier,
                                   type,
                                   payload,
                                   Map.of("k", "v")),
                7
        );

        var queryRequest = QueryConverter.convertQueryMessage(qm, clientId, componentName);

        assertThat(queryRequest).isNotNull();
        assertThat(queryRequest.getClientId()).isEqualTo(clientId);
        assertThat(queryRequest.getComponentName()).isEqualTo(componentName);
        assertThat(queryRequest.getMessageIdentifier()).isEqualTo(messageIdentifier);
        assertThat(queryRequest.getQuery()).isEqualTo("QueryType");
        assertThat(queryRequest.getPayload().getType()).isEqualTo("QueryType");
        assertThat(queryRequest.getPayload().getRevision()).isEqualTo("1");
        assertThat(queryRequest.getPayload().getData().toByteArray()).isEqualTo(payload);
        assertThat(queryRequest.getMetaDataMap().get("k").getTextValue()).isEqualTo("v");
        assertThat(queryRequest.getProcessingInstructionsList())
                .anySatisfy(pi -> assertThat(pi.getKey()).isEqualTo(PRIORITY))
                .anySatisfy(pi -> assertThat(pi.getKey()).isEqualTo(NR_OF_RESULTS))
                .anySatisfy(pi -> assertThat(pi.getKey()).isEqualTo(TIMEOUT))
                .anySatisfy(pi -> assertThat(pi.getKey()).isEqualTo(CLIENT_SUPPORTS_STREAMING));
    }

    @Test
    void convertsQueryResponseToQueryResponseMessage() {
        String exObjectPayload = "exObjectPayload";
        var response = newBuilder()
                .setMessageIdentifier(messageIdentifier)
                .setPayload(SerializedObject.newBuilder()
                                            .setType("java.lang.String")
                                            .setRevision("1")
                                            .setData(ByteString.copyFrom("ok".getBytes()))
                                            .build()
                )
                .putMetaData("m", MetaDataValue.newBuilder()
                                               .setTextValue("v")
                                               .build()
                )
                .build();
        when(converter.convert(any(), eq((Type)String.class)))
                .thenReturn(exObjectPayload);

        var responseMessage = QueryConverter.convertQueryResponse(response, converter);

        assertThat(responseMessage).isNotNull();
        assertThat(responseMessage.identifier()).isEqualTo(messageIdentifier);
        assertThat(responseMessage.type().name()).isEqualTo("java.lang.String");
        assertThat(responseMessage.payloadAs(byte[].class)).isEqualTo("ok".getBytes());
        assertThat(responseMessage.metadata()).containsEntry("m", "v");
        assertThat(responseMessage.payloadAs(String.class)).isEqualTo(exObjectPayload);

        verify(converter).convert(responseMessage.payload(), (Type) String.class);
    }

    @Test
    void convertsQueryResponseMessageToQueryResponse() {
        var type = new MessageType("java.lang.String", "1");
        var message = new GenericMessage(messageIdentifier, type, "ok".getBytes(), Map.of("m", "v"));
        var qrm = new GenericQueryResponseMessage(message);

        var response = QueryConverter.convertQueryResponseMessage("req-1", qrm);

        assertThat(response.getRequestIdentifier()).isEqualTo("req-1");
        assertThat(response.getMessageIdentifier()).isEqualTo(messageIdentifier);
        assertThat(response.getPayload().getType()).isEqualTo("java.lang.String");
        assertThat(response.getMetaDataMap()).containsKey("m");
        assertThat(response.getMetaDataMap().get("m").getTextValue()).isEqualTo("v");
    }

    @Test
    void convertsSubscriptionQueryToSubscriptionQueryMessage() {
        String exObjectPayload = "exObjectPayload";
        var qr = QueryRequest.newBuilder()
                             .setPayload(SerializedObject.newBuilder()
                                                         .setType("QueryType")
                                                         .setRevision("1")
                                                         .setData(ByteString.copyFrom(payload))
                                                         .build())
                             .build();
        var subscriptionQuery = io.axoniq.axonserver.grpc.query.SubscriptionQuery.newBuilder()
                                                                                 .setSubscriptionIdentifier(
                                                                                         messageIdentifier)
                                                                                 .setQueryRequest(qr)
                                                                                 .build();
        when(converter.convert(any(), eq((Type)String.class)))
                .thenReturn(exObjectPayload);

        var sqm = QueryConverter.convertSubscriptionQueryMessage(subscriptionQuery, converter);

        assertThat(sqm).isNotNull();
        assertThat(sqm.identifier()).isEqualTo(messageIdentifier);
        assertThat(sqm.type().name()).isEqualTo("QueryType");
        assertThat(sqm.payloadAs(String.class)).isEqualTo(exObjectPayload);

        verify(converter).convert(sqm.payload(), (Type) String.class);
    }

    @Test
    void convertsSubscriptionQueryUpdateMessageToQueryUpdate() {
        var type = new MessageType("java.lang.String", "1");
        var updateMessage = new GenericSubscriptionQueryUpdateMessage(
                new GenericMessage(messageIdentifier, type, "ok".getBytes(), Map.of("m", "v"))
        );

        var queryUpdate = QueryConverter.convertQueryUpdate(updateMessage);

        assertThat(queryUpdate.getMessageIdentifier()).isEqualTo(messageIdentifier);
        assertThat(queryUpdate.getPayload().getType()).isEqualTo("java.lang.String");
        assertThat(queryUpdate.getMetaDataMap()).containsKey("m");
        assertThat(queryUpdate.getMetaDataMap().get("m").getTextValue()).isEqualTo("v");
    }

    @Test
    void convertsQueryUpdateToSubscriptionQueryUpdateMessage() {
        String exObjectPayload = "exObjectPayload";
        var qu = QueryUpdate.newBuilder()
                            .setMessageIdentifier(messageIdentifier)
                            .setPayload(SerializedObject.newBuilder()
                                                        .setType("java.lang.String")
                                                        .setRevision("1")
                                                        .setData(ByteString.copyFrom("ok".getBytes()))
                                                        .build())
                            .putMetaData("m", MetaDataValue.newBuilder().setTextValue("v").build())
                            .build();

        when(converter.convert(any(), eq((Type) String.class)))
                .thenReturn(exObjectPayload);

        var updateMessage = QueryConverter.convertQueryUpdate(qu, converter);

        assertThat(updateMessage.identifier()).isEqualTo(messageIdentifier);
        assertThat(updateMessage.type().name()).isEqualTo("java.lang.String");
        assertThat(updateMessage.metadata()).containsEntry("m", "v");
        assertThat(updateMessage.payloadAs(byte[].class)).isEqualTo("ok".getBytes());
        assertThat(updateMessage.payloadAs(String.class)).isEqualTo(exObjectPayload);

        verify(converter).convert(updateMessage.payload(), (Type) String.class);
    }

    @Test
    void convertsClientAndThrowableToErrorQueryUpdate() {
        var throwable = new RuntimeException("boom");
        var qu = QueryConverter.convertQueryUpdate(clientId, ErrorCode.QUERY_EXECUTION_ERROR, throwable, null);

        assertThat(qu.getClientId()).isEqualTo(clientId);
        assertThat(qu.hasErrorMessage()).isTrue();
        assertThat(qu.getErrorMessage().getMessage()).contains("boom");
        assertThat(qu.getErrorMessage().getErrorCode()).isEqualTo("AXONIQ-5001");
        assertThat(qu.getErrorCode()).isEqualTo("AXONIQ-5001");
        assertThat(qu.hasPayload()).isFalse();
    }

    @Test
    void convertsClientAndThrowableWithDetailsToErrorQueryUpdateWithPayload() {
        // given a QueryExecutionException carrying application-specific details
        var throwable = new QueryExecutionException("boom", null, "details");
        when(converter.convert("details", byte[].class)).thenReturn("details".getBytes());

        // when
        var qu = QueryConverter.convertQueryUpdate(clientId, ErrorCode.QUERY_EXECUTION_ERROR, throwable, converter);

        // then
        assertThat(qu.hasPayload()).isTrue();
        assertThat(qu.getPayload().getData().toStringUtf8()).isEqualTo("details");
        assertThat(qu.getPayload().getType()).isEqualTo(String.class.getName());

        verify(converter).convert("details", byte[].class);
    }

    @Test
    void roundTripsHandlerExecutionDetailsThroughConvertToAxonException() {
        // given a handler-side exception carrying application-specific details
        var throwable = new QueryExecutionException("boom", null, "details");
        when(converter.convert("details", byte[].class)).thenReturn("details".getBytes());

        // when the error update produced on the send side is parsed again on the receive side
        var queryUpdate = QueryConverter.convertQueryUpdate(clientId, ErrorCode.QUERY_EXECUTION_ERROR, throwable,
                                                            converter);
        byte[] rawDetails = queryUpdate.getPayload().getData().toByteArray();
        when(converter.convert(rawDetails, (Type) String.class)).thenReturn("details");

        AxonException result = ExceptionConverter.convertToAxonException(
                queryUpdate.getErrorCode(), queryUpdate.getErrorMessage(), queryUpdate.getPayload(), converter
        );

        // then the original details are recovered through the round trip
        assertThat(result).isInstanceOf(QueryExecutionException.class);
        Optional<String> details = ((QueryExecutionException) result).getDetails(String.class);
        assertThat(details).hasValue("details");

        verify(converter).convert("details", byte[].class);
        verify(converter).convert(rawDetails, (Type) String.class);
    }

    @Test
    void convertsQueryMessageWithoutPriorityToQueryRequestWithoutPriorityInstruction() {
        // given a query message without an explicit priority
        var qm = new GenericQueryMessage(
                new GenericMessage(messageIdentifier, new MessageType("QueryType", "1"), payload, Map.of())
        );

        // when
        var queryRequest = QueryConverter.convertQueryMessage(qm, clientId, componentName);

        // then
        assertThat(queryRequest.getProcessingInstructionsList())
                .noneSatisfy(pi -> assertThat(pi.getKey()).isEqualTo(PRIORITY));
    }

    @Test
    void convertsQueryRequestWithEmptyRevisionToDefaultVersion() {
        // given a query request whose payload carries no revision
        var queryRequest = QueryRequest.newBuilder()
                                       .setMessageIdentifier(messageIdentifier)
                                       .setPayload(SerializedObject.newBuilder()
                                                                   .setType("QueryType")
                                                                   .setData(ByteString.copyFrom(payload))
                                                                   .build())
                                       .build();

        // when
        var queryMessage = QueryConverter.convertQueryRequest(queryRequest, converter);

        // then absent revision defaults to the default version and absent priority to the lowest priority
        assertThat(queryMessage.type().version()).isEqualTo(MessageType.DEFAULT_VERSION);
        assertThat(queryMessage.priority()).hasValue(0);
    }

    @Test
    void convertsQueryResponseWithEmptyRevisionToDefaultVersion() {
        // given a query response whose payload carries no revision
        var response = newBuilder()
                .setMessageIdentifier(messageIdentifier)
                .setPayload(SerializedObject.newBuilder()
                                            .setType("java.lang.String")
                                            .setData(ByteString.copyFrom("ok".getBytes()))
                                            .build())
                .build();

        // when
        var responseMessage = QueryConverter.convertQueryResponse(response, converter);

        // then
        assertThat(responseMessage.type().version()).isEqualTo(MessageType.DEFAULT_VERSION);
    }

    @Test
    void convertsQueryResponseMessageWithNullPayloadToEmptyPayloadData() {
        // given a query response message without a payload
        var message = new GenericMessage(messageIdentifier, new MessageType("java.lang.String", "1"), null, Map.of());
        var qrm = new GenericQueryResponseMessage(message);

        // when
        var response = QueryConverter.convertQueryResponseMessage("req-1", qrm);

        // then
        assertThat(response.getPayload().getType()).isEqualTo("java.lang.String");
        assertThat(response.getPayload().getData().isEmpty()).isTrue();
    }

    @Test
    void convertsSubscriptionQueryUpdateMessageWithNullPayloadToEmptyPayloadData() {
        // given an update message without a payload
        var updateMessage = new GenericSubscriptionQueryUpdateMessage(
                new GenericMessage(messageIdentifier, new MessageType("java.lang.String", "1"), null, Map.of())
        );

        // when
        var queryUpdate = QueryConverter.convertQueryUpdate(updateMessage);

        // then
        assertThat(queryUpdate.getPayload().getType()).isEqualTo("java.lang.String");
        assertThat(queryUpdate.getPayload().getData().isEmpty()).isTrue();
    }

    @Test
    void convertsClientAndThrowableWithoutErrorCodeToErrorQueryUpdateWithoutErrorCode() {
        // given
        var throwable = new RuntimeException("boom");

        // when
        var qu = QueryConverter.convertQueryUpdate(clientId, null, throwable, null);

        // then
        assertThat(qu.getClientId()).isEqualTo(clientId);
        assertThat(qu.hasErrorMessage()).isTrue();
        assertThat(qu.getErrorMessage().getMessage()).contains("boom");
        assertThat(qu.getErrorCode()).isEmpty();
        assertThat(qu.getErrorMessage().getErrorCode()).isEmpty();
    }

    @Test
    void convertQueryMessageThrowsOnNonByteArrayPayload() {
        var type = new MessageType("QueryType", "1");
        var qm = new GenericQueryMessage(
                new GenericMessage(type, "not-bytes", Metadata.emptyInstance())
        );

        assertThatThrownBy(() -> QueryConverter.convertQueryMessage(qm, clientId, componentName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Payload must be of type byte[]");
    }

    @Test
    void convertQueryResponseThrowsOnErrorMessage() {
        var response = newBuilder()
                .setErrorMessage(ErrorMessage.newBuilder()
                                             .setMessage("err")
                                             .build()
                )
                .build();

        assertThatThrownBy(() -> QueryConverter.convertQueryResponse(response, converter))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contained an error");
    }
}
