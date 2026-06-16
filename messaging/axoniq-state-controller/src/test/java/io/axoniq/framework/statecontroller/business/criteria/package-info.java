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


/**
 * Value-level acceptance tests for the fluent multi-term {@code EventCriteria} builder on the business-first
 * {@code History} surface. Exercises the tagless type-first start, the tag-then-types {@code either(...)} fold of the
 * real DCB target, the bare {@code of(tag)} all-types regression, and the builder mutation guards against seeded,
 * tagged events over an in-memory event store.
 */
@NullMarked
package io.axoniq.framework.statecontroller.business.criteria;

import org.jspecify.annotations.NullMarked;
