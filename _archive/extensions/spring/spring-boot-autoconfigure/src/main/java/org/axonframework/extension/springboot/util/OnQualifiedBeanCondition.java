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

package org.axonframework.extension.springboot.util;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.context.annotation.Condition;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * {@link Condition} implementation to check for a bean instance of a specific class *and* a specific qualifier on it.
 */
@Order(Ordered.LOWEST_PRECEDENCE)
public class OnQualifiedBeanCondition extends AbstractQualifiedBeanCondition {

    public OnQualifiedBeanCondition() {
        super(ConditionalOnQualifiedBean.class.getName(), "beanClass", "qualifier");
    }

    @Override
    protected ConditionOutcome buildOutcome(boolean anyMatch, String message) {
        return new ConditionOutcome(anyMatch, message);
    }

}
