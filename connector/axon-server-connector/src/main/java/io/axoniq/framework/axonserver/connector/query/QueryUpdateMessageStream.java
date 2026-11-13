/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.query;

import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import org.axonframework.common.AxonException;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.jspecify.annotations.Nullable;

import static io.axoniq.framework.axonserver.connector.query.QueryConverter.convertQueryUpdate;
import static io.axoniq.framework.axonserver.connector.shared.ExceptionConverter.convertToAxonException;

/**
 * A specialized implementation of {@link AbstractQueryResponseMessageStream} that processes a stream of
 * {@link QueryUpdate} objects and transforms them into {@link QueryResponseMessage} instances. This class is used to
 * handle query update, including error handling and response message creation.
 * <p/>
 * This class relies on its abstract superclass to manage the underlying {@link ResultStream}, implementing
 * functionality specific to {@link QueryUpdate} to determine whether a message represents an error and to transform
 * such update into structured responses or exceptions.
 */
public class QueryUpdateMessageStream extends AbstractQueryResponseMessageStream<QueryUpdate> {

    private final @Nullable MessageConverter converter;

    /**
     * Initializes a new instance of the {@code QueryResponseMessageStream} which wraps a {@link ResultStream} of
     * {@link QueryUpdate} objects.
     *
     * @param stream the {@link ResultStream} of {@link QueryUpdate} instances to be wrapped; must not be null. If
     *               {@code null}, a {@link NullPointerException} will be thrown.
     * @param converter the converter used for {@link QueryResponseMessage} inline payload conversion
     */
    public QueryUpdateMessageStream(ResultStream<QueryUpdate> stream, @Nullable MessageConverter converter) {
        super(stream);
        this.converter = converter;
    }

    @Override
    protected QueryResponseMessage buildResponseMessage(QueryUpdate queryUpdate) {
        return convertQueryUpdate(queryUpdate, converter);
    }

    @Override
    protected AxonException createAxonException(QueryUpdate queryUpdate) {
        return convertToAxonException(queryUpdate.getErrorCode(),
                                      queryUpdate.getErrorMessage(),
                                      queryUpdate.getPayload(),
                                      converter);
    }

    @Override
    protected boolean isError(QueryUpdate queryUpdate) {
        return queryUpdate.hasErrorMessage();
    }
}
