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

package org.axonframework.messaging.core;

import org.axonframework.messaging.core.Context.ResourceKey;

/**
 * Test class validating the {@link SimpleContext}.
 *
 * @author Steven van Beelen
 */
public class SimpleContextTest extends ContextTestSuite<SimpleContext> {

    private static final ResourceKey<String> RANDOM_RESOURCE_KEY = ResourceKey.withLabel("RandomResource");

    @Override
    public SimpleContext testSubject() {
        return new SimpleContext(RANDOM_RESOURCE_KEY, "SimpleContext");
    }
}
