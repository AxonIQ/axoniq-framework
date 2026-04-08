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

package org.axonframework.messaging.core.retry;

import org.axonframework.common.AxonNonTransientException;

/**
 * An Axon-specific {@link java.util.function.Predicate}, used to check the non-transiency of a failure comparing it against concrete classes.
 * <p/>
 * This implementation uses only {@link AxonNonTransientException} class for comparisons.
 * <p/>
 * This {@code Predicate} acts as the default for {@link RetryScheduler} instances.
 *
 * @author Damir Murat
 * @since 4.6.0
 */
public class AxonNonTransientExceptionClassesPredicate extends NonTransientExceptionClassesPredicate {
    public AxonNonTransientExceptionClassesPredicate() {
        super(AxonNonTransientException.class);
    }
}
