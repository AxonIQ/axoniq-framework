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

package io.axoniq.framework.tracing;

import org.axonframework.messaging.core.Message;

/**
 * Span-name conventions shared by the built-in tracing decorators and available to external decorator authors.
 * <p>
 * Names follow the pattern {@code "<Component>.<operation> <messageName>"}, where {@code <messageName>} is the
 * payload's {@link org.axonframework.messaging.core.QualifiedName}. Keeping these conventions in one place keeps the
 * span tree consistent across components and across modules that add their own tracing.
 * <p>
 * The constants on this class are grown incrementally as tracing slices land; the command-bus names are present from
 * the first slice.
 *
 * @author Mateusz Nowak
 * @since 5.2.0
 */
public final class SpanNames {

    /** Prefix for the command-dispatch span. */
    public static final String COMMAND_DISPATCH = "CommandBus.dispatchCommand";

    /** Prefix for the command-handling span. */
    public static final String COMMAND_HANDLE = "CommandBus.handleCommand";

    private SpanNames() {
    }

    /**
     * Returns the span name for dispatching the given command message.
     *
     * @param command the command being dispatched
     * @return the command-dispatch span name
     */
    public static String commandDispatch(Message command) {
        return COMMAND_DISPATCH + " " + messageName(command);
    }

    /**
     * Returns the span name for handling the given command message.
     *
     * @param command the command being handled
     * @return the command-handle span name
     */
    public static String commandHandle(Message command) {
        return COMMAND_HANDLE + " " + messageName(command);
    }

    /**
     * Returns the version-less qualified name of the given message's type, used as the trailing element of a span
     * name.
     *
     * @param message the message whose name is rendered
     * @return the message's qualified name
     */
    public static String messageName(Message message) {
        return message.type().qualifiedName().name();
    }
}
