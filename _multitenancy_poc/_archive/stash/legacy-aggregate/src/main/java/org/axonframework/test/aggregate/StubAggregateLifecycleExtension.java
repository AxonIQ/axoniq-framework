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

package org.axonframework.test.aggregate;

import org.junit.jupiter.api.extension.*;

/**
 * Implementation of {@link StubAggregateLifecycle} that can be used as an {@link org.junit.jupiter.api.extension.RegisterExtension}
 * annotated method or field in a test class. In that case, the JUnit lifecycle will automatically register and
 * deregister the {@code StubAggregateLifecycle}.
 * <p>
 * Usage example:
 * <pre>
 * &#064;RegisterExtension
 * public StubAggregateLifecycleExtension lifecycle = new StubAggregateLifecycleExtension();
 *
 * &#064;Test
 * public void testMethod() {
 *     ... perform tests ...
 *
 *     // get applied events from lifecycle to validate some more
 *     lifecycle.getAppliedEvents();
 * }
 * </pre>
 */
public class StubAggregateLifecycleExtension extends StubAggregateLifecycle
        implements AfterEachCallback, BeforeEachCallback {

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        activate();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        close();
    }
}
