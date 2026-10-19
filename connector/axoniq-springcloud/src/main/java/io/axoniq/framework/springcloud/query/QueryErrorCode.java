/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.ExceptionUtils;
import org.axonframework.messaging.queryhandling.NoHandlerForQueryException;

/**
 * What kind of failure ended a query's response stream, as reported to the member that dispatched the query.
 * <p>
 * The distinction that matters to the dispatching member is whether trying again could succeed. A query no member
 * handles yet may be handled once one finishes starting up, while a query whose handler rejected it will be rejected
 * again by the same input.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public enum QueryErrorCode {

    /**
     * The member had no handler for the query.
     */
    NO_HANDLER_FOR_QUERY,

    /**
     * The query's handler failed, in a way that trying again may not repeat.
     */
    QUERY_EXECUTION_ERROR,

    /**
     * The query's handler failed in a way that will repeat, so trying again is pointless.
     * <p>
     * A query the answering member could not even read is reported this way too: it never reached a handler, and
     * sending the same bytes again cannot change that.
     */
    QUERY_EXECUTION_NON_TRANSIENT_ERROR;

    /**
     * Classifies the given {@code cause} as the code to report it under.
     *
     * @param cause the failure to classify
     * @return the code the given {@code cause} is reported under
     */
    public static QueryErrorCode classify(Throwable cause) {
        if (ExceptionUtils.findException(cause, NoHandlerForQueryException.class).isPresent()) {
            return NO_HANDLER_FOR_QUERY;
        }
        return ExceptionUtils.isExplicitlyNonTransient(cause)
                ? QUERY_EXECUTION_NON_TRANSIENT_ERROR
                : QUERY_EXECUTION_ERROR;
    }
}
