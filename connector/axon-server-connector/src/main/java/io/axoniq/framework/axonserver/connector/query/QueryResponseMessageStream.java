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

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.axonframework.common.AxonException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import static io.axoniq.framework.axonserver.connector.query.QueryConverter.convertQueryResponse;
import static io.axoniq.framework.axonserver.connector.shared.ExceptionConverter.convertToAxonException;

/**
 * A {@link MessageStream} implementation that wraps an {@link ResultStream} of {@link QueryResponse}s, using
 * {@link QueryConverter}.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public class QueryResponseMessageStream extends AbstractQueryResponseMessageStream<QueryResponse> {

    private final @Nullable MessageConverter converter;

    /**
     * Initializes a new instance of the {@code QueryResponseMessageStream} which wraps a {@link ResultStream} of
     * {@link QueryResponse} objects.
     *
     * @param stream the {@link ResultStream} of {@link QueryResponse} instances to be wrapped; must not be null. If
     *               {@code null}, a {@link NullPointerException} will be thrown.
     * @param converter the converter to be used for payload conversion
     */
    public QueryResponseMessageStream(ResultStream<QueryResponse> stream, @Nullable MessageConverter converter) {
        super(stream);
        this.converter = converter;
    }

    @Override
    protected QueryResponseMessage buildResponseMessage(QueryResponse queryResponse) {
        return convertQueryResponse(queryResponse, converter);
    }

    @Override
    protected AxonException createAxonException(QueryResponse queryResponse) {
        return convertToAxonException(queryResponse.getErrorCode(),
                                      queryResponse.getErrorMessage(),
                                      queryResponse.getPayload());
    }

    @Override
    protected boolean isError(QueryResponse queryResponse) {
        return queryResponse.hasErrorMessage();
    }

    @Override
    protected boolean isEmptyResult(QueryResponse queryResponse) {
        return QueryConverter.EMPTY_PAYLOAD_TYPE.equalsIgnoreCase(queryResponse.getPayload().getType());
    }
}
