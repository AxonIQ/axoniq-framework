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
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.framework.axonserver.connector.shared.ErrorCode;
import io.axoniq.framework.axonserver.connector.shared.ExceptionConverter;
import io.axoniq.framework.axonserver.connector.shared.MetadataConverter;
import io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.GenericQueryResponseMessage;
import org.axonframework.messaging.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SubscriptionQueryUpdateMessage;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.util.ProcessingInstructionUtils.createProcessingInstruction;

/**
 * Utility class to convert queries during
 * {@link AxonServerQueryBusConnector#query(QueryMessage, ProcessingContext) dispatching} and handling of
 * {@link AxonServerQueryBusConnector#subscribe(QualifiedName) subscribed} query handlers in the
 * {@link AxonServerQueryBusConnector}.
 * <p>
 * This utility class is marked as {@link Internal} as it is specific for the {@link AxonServerQueryBusConnector}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public final class QueryConverter {

    /**
     * The {@link SerializedObject#getType()} used to mark a {@link QueryResponse} as carrying no payload.
     * <p>
     * This mirrors the {@code "empty"} sentinel type Axon Framework 4 has always used for a {@code null} query result,
     * so that an Axon Framework 4 client dispatching a direct query recognizes the response as "no result" instead of
     * failing on a payload it cannot deserialize.
     *
     * @deprecated as this constant purely exists for interoperability between Axon Framework 4 and Axon Framework 5
     */
    @Deprecated(forRemoval = true, since = "5.2.1")
    static final String EMPTY_PAYLOAD_TYPE = "empty";

    /**
     * Converts a {@link QueryRequest} into a {@link QueryMessage}.
     * <p/>
     * This method processes the given QueryRequest by extracting its payload, metadata, and other relevant fields to
     * construct a QueryMessage that represents the request for querying information.
     *
     * @param queryRequest the {@link QueryRequest} to be converted into a {@link QueryMessage} (must not be null)
     * @param converter    the converter to be used for payload conversion
     * @return a {@link QueryMessage} representation of the provided {@link QueryRequest}. The returned object contains
     * the extracted payload, metadata, and expected response type
     * @throws NullPointerException if the provided {@link QueryRequest} is null
     */
    static QueryMessage convertQueryRequest(QueryRequest queryRequest, @Nullable Converter converter) {
        var payload = queryRequest.getPayload();
        Integer priority = ProcessingInstructionUtils.priority(queryRequest.getProcessingInstructionsList());

        String revision = revisionOrDefault(payload);
        var type = new MessageType(payload.getType(), revision);
        return new GenericQueryMessage(
                new GenericMessage(
                        queryRequest.getMessageIdentifier(),
                        type,
                        payload.getData().toByteArray(),
                        MetadataConverter.convertMetadataValuesToGrpc(queryRequest.getMetaDataMap())
                ),
                priority
        ).withConverter(converter);
    }

    /**
     * Converts a {@link QueryMessage} into a {@link QueryRequest}.
     * <p/>
     * This method processes the provided {@link QueryMessage} and constructs a corresponding {@link QueryRequest} with
     * all necessary details, such as metadata, payload, and other processing instructions.
     *
     * @param query         the {@link QueryMessage} to be converted. Must not be null. The payload must be of type
     *                      {@code byte[]}, otherwise an {@link IllegalArgumentException} is thrown
     * @param clientId      the identifier of the client making the query. Must not be null
     * @param componentName the name of the component handling the query. Must not be null
     * @return a {@link QueryRequest} That represents the provided {@link QueryMessage}. Contains the extracted
     * metadata, payload, and other specific configurations from the original message.
     * @throws IllegalArgumentException if the payload of the {@link QueryMessage} is not of type {@code byte[]}
     */
    public static QueryRequest convertQueryMessage(QueryMessage query,
                                                   String clientId,
                                                   String componentName) {
        Object payload = query.payload();
        if (!(payload instanceof byte[] payloadAsBytes)) {
            throw new IllegalArgumentException(
                    "Payload must be of type byte[] for AxonServerConnector, but was: "
                            + query.payloadType().getName()
                            + ", consider using a Converter-based QueryBusConnector"
            );
        }
        QueryRequest.Builder builder = QueryRequest.newBuilder();
        addPriority(builder, query);

        return builder.setTimestamp(System.currentTimeMillis())
                      .setClientId(clientId)
                      .setComponentName(componentName)
                      .setMessageIdentifier(query.identifier())
                      .setQuery(query.type().name())
                      .putAllMetaData(MetadataConverter.convertGrpcToMetadataValues(query.metadata()))
                      .setPayload(SerializedObject.newBuilder()
                                                  .setData(ByteString.copyFrom(payloadAsBytes))
                                                  .setType(query.type().name())
                                                  .setRevision(query.type().version())
                                                  .build())
                      // TODO #resultHandlersToHit/cardinality is always 1. Is this required?
                      .addProcessingInstructions(nrOfResults(1))
                      // TODO Defaulted to 1H. Do we need this?
                      .addProcessingInstructions(timeout(TimeUnit.HOURS.toMillis(1)))
                      .addProcessingInstructions(supportsStreaming())
                      .build();
    }

    /**
     * Converts a {@link QueryResponse} into a {@link QueryResponseMessage}.
     * <p/>
     * This method processes the given {@link QueryResponse}, extracts its payload, metadata, and other necessary
     * components to construct a corresponding {@link QueryResponseMessage}. If the {@link QueryResponse} contains an
     * error, an appropriate exception is included in the resulting {@link QueryResponseMessage}.
     *
     * @param queryResponse the {@link QueryResponse} to be converted (must not be null). The {@link QueryResponse}
     *                      should contain valid payload and metadata details necessary to construct the resulting
     *                      {@link QueryResponseMessage}.
     * @param converter     the converter to be used for payload conversion
     * @return A {@link QueryResponseMessage} representation of the provided {@link QueryResponse}. The returned message
     * includes the processed payload, metadata, and any error information, if applicable.
     * @throws IllegalArgumentException if the provided {@link QueryResponse} contains an error, in which case we use
     *                                  {@link ExceptionConverter#convertToAxonException(String, ErrorMessage,
     *                                  SerializedObject, Converter)}.
     */
    public static QueryResponseMessage convertQueryResponse(QueryResponse queryResponse,
                                                            @Nullable Converter converter) {
        if (queryResponse.hasErrorMessage()) {
            throw new IllegalArgumentException("Query Response contained an error.");
        }
        SerializedObject responsePayload = queryResponse.getPayload();
        String revision = revisionOrDefault(responsePayload);
        var message = new GenericMessage(
                queryResponse.getMessageIdentifier(),
                new MessageType(responsePayload.getType(), revision),
                responsePayload.getData().toByteArray(),
                MetadataConverter.convertMetadataValuesToGrpc(queryResponse.getMetaDataMap())
        );

        return new GenericQueryResponseMessage(message)
                .withConverter(converter);
    }

    /**
     * Converts a {@link QueryResponseMessage} into a {@link QueryResponse}.
     * <p>
     * This method processes the provided {@link QueryResponseMessage} to construct a corresponding
     * {@link QueryResponse}. The resulting {@link QueryResponse} contains the message identifier, request identifier,
     * processed payload, metadata, and other necessary information.
     *
     * @param requestId            the {@link QueryMessage#identifier()} that initiated the query. Used to associate the
     *                             resulting {@link QueryResponse} with the original request. Must not be null
     * @param queryResponseMessage the {@link QueryResponseMessage} to be converted into a {@link QueryResponse}. Must
     *                             not be null
     * @return a {@link QueryResponse} representation of the provided {@link QueryResponseMessage}. It includes the
     * identifier, metadata, and the serialized payload
     */
    public static QueryResponse convertQueryResponseMessage(String requestId,
                                                            QueryResponseMessage queryResponseMessage) {
        byte[] payload = Objects.requireNonNullElseGet(queryResponseMessage.payloadAs(byte[].class), () -> new byte[0]);
        return QueryResponse.newBuilder()
                            .setMessageIdentifier(queryResponseMessage.identifier())
                            .setRequestIdentifier(requestId)
                            .setPayload(SerializedObject.newBuilder()
                                                        .setType(queryResponseMessage.type().name())
                                                        .setRevision(queryResponseMessage.type().version())
                                                        .setData(ByteString.copyFrom(payload))
                                                        .build()
                            )
                            .putAllMetaData(MetadataConverter.convertGrpcToMetadataValues(queryResponseMessage.metadata()))
                            .build();
    }

    /**
     * Constructs a {@link QueryResponse} carrying no payload, marked with the {@link #EMPTY_PAYLOAD_TYPE} sentinel.
     * <p>
     * Used when a direct query is handled without producing a single result (e.g. a {@code @QueryHandler} returning
     * {@code null} or {@link java.util.Optional#empty()}), so that exactly one response is still sent for the query, as
     * required by the Axon Framework 4 wire protocol.
     *
     * @param requestId the {@link QueryMessage#identifier()} that initiated the query. Used to associate the resulting
     *                  {@link QueryResponse} with the original request. Must not be null.
     * @return a {@link QueryResponse} with an empty payload, marked with the {@link #EMPTY_PAYLOAD_TYPE} sentinel
     * @deprecated as this method purely exists for interoperability between Axon Framework 4 and Axon Framework 5
     */
    @Deprecated(forRemoval = true, since = "5.2.1")
    static QueryResponse emptyQueryResponse(String requestId) {
        return QueryResponse.newBuilder()
                            .setMessageIdentifier(IdentifierFactory.getInstance().generateIdentifier())
                            .setRequestIdentifier(requestId)
                            .setPayload(SerializedObject.newBuilder()
                                                        .setType(EMPTY_PAYLOAD_TYPE)
                                                        .setData(ByteString.EMPTY)
                                                        .build())
                            .build();
    }

    /**
     * Converts a {@link SubscriptionQuery} into a {@link QueryMessage}. This method processes the given
     * {@link SubscriptionQuery} by extracting its identifier, payload, metadata, and other necessary fields to
     * construct a {@link QueryMessage}.
     *
     * @param query     the {@link SubscriptionQuery} to be converted. Must not be null. The {@link SubscriptionQuery}
     *                  should contain properly set payload, metadata, and response type details
     * @param converter the converter to be used for payload conversion
     * @return a {@link QueryMessage} representation of the provided {@link SubscriptionQuery}. It includes the
     * processed payload, metadata, and response type
     * @throws NullPointerException if the provided {@link SubscriptionQuery} is null
     */
    public static QueryMessage convertSubscriptionQueryMessage(SubscriptionQuery query, @Nullable Converter converter) {
        SerializedObject responsePayload = query.getQueryRequest().getPayload();
        String revision = revisionOrDefault(responsePayload);
        var message = new GenericMessage(
                query.getSubscriptionIdentifier(),
                new MessageType(responsePayload.getType(), revision),
                responsePayload.getData().toByteArray(),
                MetadataConverter.convertMetadataValuesToGrpc(query.getQueryRequest().getMetaDataMap())
        );

        return new GenericQueryMessage(message)
                .withConverter(converter);
    }


    /**
     * Converts a {@link SubscriptionQueryUpdateMessage} into a {@link QueryUpdate}. This method processes the provided
     * {@link SubscriptionQueryUpdateMessage} by extracting its payload, metadata, and other relevant details to
     * construct a {@link QueryUpdate}. If the payload is unavailable, it defaults to an empty byte array.
     *
     * @param update the {@link SubscriptionQueryUpdateMessage} to be converted. Must not be null. The payload and
     *               metadata should be properly set in the provided update
     * @return a {@link QueryUpdate} representation of the given {@link SubscriptionQueryUpdateMessage}. It includes the
     * extracted payload, metadata, and other necessary information
     * @throws NullPointerException if the provided {@link SubscriptionQueryUpdateMessage} is null
     */
    public static QueryUpdate convertQueryUpdate(SubscriptionQueryUpdateMessage update) {
        byte[] payload = Objects.requireNonNullElseGet(update.payloadAs(byte[].class), () -> new byte[0]);
        return QueryUpdate.newBuilder()
                          .setMessageIdentifier(update.identifier())
                          .setPayload(SerializedObject.newBuilder()
                                                      .setType(update.type().name())
                                                      .setRevision(update.type().version())
                                                      .setData(ByteString.copyFrom(payload))
                                                      .build())
                          .putAllMetaData(MetadataConverter.convertGrpcToMetadataValues(update.metadata()))
                          .build();
    }

    /**
     * Converts a {@link QueryUpdate} object into a {@link SubscriptionQueryUpdateMessage}.
     *
     * @param queryUpdate the {@link QueryUpdate} object to be converted; must not be null
     * @param converter   the converter to be used for payload conversion
     * @return a {@link SubscriptionQueryUpdateMessage} created from the provided {@link QueryUpdate}
     */
    public static SubscriptionQueryUpdateMessage convertQueryUpdate(QueryUpdate queryUpdate,
                                                                    @Nullable Converter converter) {
        SerializedObject payload = queryUpdate.getPayload();
        String revision = revisionOrDefault(payload);
        var message = new GenericMessage(
                queryUpdate.getMessageIdentifier(),
                new MessageType(payload.getType(), revision),
                payload.getData().toByteArray(),
                MetadataConverter.convertMetadataValuesToGrpc(queryUpdate.getMetaDataMap())
        );

        return new GenericSubscriptionQueryUpdateMessage(message)
                .withConverter(converter);
    }

    /**
     * Converts error information and a client identifier into a {@link QueryUpdate}. This method creates a
     * {@link QueryUpdate} containing the error message derived from the given {@link Throwable} along with the provided
     * client identifier.
     *
     * @param clientId  the identifier of the client associated with the error. Must not be null
     * @param errorCode the error code identifying the type of action that resulted in an error, if known
     * @param error     the {@link Throwable} containing error details to be translated into an error message. Must not
     *                  be null
     * @param converter the {@link Converter} used to serialize application-specific exception details, if present, or
     *                  {@code null} if none is available
     * @return a {@link QueryUpdate} containing the client identifier and an error message derived from the provided
     * {@link Throwable}
     */
    public static QueryUpdate convertQueryUpdate(String clientId,
                                                 @Nullable ErrorCode errorCode,
                                                 Throwable error,
                                                 @Nullable Converter converter) {
        QueryUpdate.Builder builder =
                QueryUpdate.newBuilder()
                           .setErrorMessage(ExceptionConverter.convertToErrorMessage(clientId, errorCode, error))
                           .setClientId(clientId);
        if (errorCode != null) {
            builder.setErrorCode(errorCode.errorCode());
        }
        SerializedObject detailsPayload = ExceptionConverter.convertToDetails(error, converter);
        if (detailsPayload != null) {
            builder.setPayload(detailsPayload);
        }
        return builder.build();
    }

    private static void addPriority(QueryRequest.Builder builder, QueryMessage query) {
        query.priority().ifPresent(priority -> {
            var instruction = createProcessingInstruction(ProcessingKey.PRIORITY,
                                                          MetaDataValue.newBuilder()
                                                                       .setNumberValue(priority));
            builder.addProcessingInstructions(instruction);
        });
    }

    /**
     * Return the {@link SerializedObject#getRevision()} or {@link MessageType#DEFAULT_VERSION} if the revision is
     * {@code empty} or {@code null}.
     *
     * @param payload the {@link SerializedObject} to resolve the revision from
     * @return the revision or the {@link MessageType#DEFAULT_VERSION} if the revision is {@code empty} or {@code null}
     */
    private static String revisionOrDefault(SerializedObject payload) {
        return StringUtils.nonEmptyOrNull(payload.getRevision())
                ? payload.getRevision()
                : MessageType.DEFAULT_VERSION;
    }

    private static ProcessingInstruction.Builder nrOfResults(int nrOfResults) {
        return createProcessingInstruction(ProcessingKey.NR_OF_RESULTS, nrOfResults);
    }

    private static ProcessingInstruction.Builder timeout(long timeout) {
        return createProcessingInstruction(ProcessingKey.TIMEOUT, timeout);
    }

    private static ProcessingInstruction.Builder supportsStreaming() {
        return createProcessingInstruction(ProcessingKey.CLIENT_SUPPORTS_STREAMING, true);
    }

    private QueryConverter() {
        // Utility class
    }
}
