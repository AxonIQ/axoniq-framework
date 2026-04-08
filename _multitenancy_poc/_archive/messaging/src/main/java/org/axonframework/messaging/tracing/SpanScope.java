/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.tracing;

/**
 * Represents the scope of a {@link Span}. This is attached to the thread, and should be closed
 * on the same thread as it was created before the span is ended.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.5
 */
@FunctionalInterface
public interface SpanScope extends AutoCloseable {
    /**
     * Closes the scope of the Span on which it was opened.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    void close();
}
