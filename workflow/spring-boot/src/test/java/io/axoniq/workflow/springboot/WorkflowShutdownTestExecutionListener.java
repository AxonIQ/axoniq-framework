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
package io.axoniq.workflow.springboot;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext.HierarchyMode;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

/**
 * Evicts the cached {@link org.springframework.context.ApplicationContext} from the Spring test context cache when the
 * test body has programmatically closed it. This prevents listeners that later call
 * {@code TestContext.publishEvent(...)} (e.g. {@code EventPublishingTestExecutionListener}) from asserting against an
 * inactive context after {@code applicationContext.close()} was invoked from inside a test method.
 * <p>
 * Ordered above {@code EventPublishingTestExecutionListener.ORDER} (10_000) so it runs first in the reverse-order
 * {@code afterXxx} chain, letting subsequent listeners see a freshly loaded context instead of the closed one.
 */
public class WorkflowShutdownTestExecutionListener extends AbstractTestExecutionListener {

    private static final int ORDER_ABOVE_EVENT_PUBLISHER = 20_000;

    @Override
    public int getOrder() {
        return ORDER_ABOVE_EVENT_PUBLISHER;
    }

    @Override
    public void afterTestExecution(TestContext testContext) {
        evictIfClosed(testContext);
    }

    @Override
    public void afterTestMethod(TestContext testContext) {
        evictIfClosed(testContext);
    }

    @Override
    public void afterTestClass(TestContext testContext) {
        evictIfClosed(testContext);
    }

    private static void evictIfClosed(TestContext testContext) {
        if (!testContext.hasApplicationContext()) {
            return;
        }
        try {
            var ctx = testContext.getApplicationContext();
            if (ctx instanceof ConfigurableApplicationContext cac && !cac.isActive()) {
                testContext.markApplicationContextDirty(HierarchyMode.EXHAUSTIVE);
            }
        } catch (IllegalStateException ex) {
            testContext.markApplicationContextDirty(HierarchyMode.EXHAUSTIVE);
        }
    }
}
