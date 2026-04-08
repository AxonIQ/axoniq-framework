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

package org.axonframework.axonserver.connector;

import org.axonframework.messaging.core.Message;

/**
 * Simple TargetContextResolver implementation  to be able to spy an instance of it for testing.
 *
 * @author Steven van Beelen
 */
public class TestTargetContextResolver<T extends Message> implements TargetContextResolver<T> {

    public static final String BOUNDED_CONTEXT = "not-important";

    @Override
    public String resolveContext(T message) {
        return BOUNDED_CONTEXT;
    }
}
