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
 * Association values, the means by which an event is correlated to the saga instances that should handle it.
 * <p>
 * An {@link org.axonframework.modelling.saga.AssociationValue} is a key-value pair that a saga registers itself under;
 * {@link org.axonframework.modelling.saga.AssociationValues} tracks a saga's current set together with the additions
 * and removals made since it was last stored, which is what a
 * {@link org.axonframework.modelling.saga.repository.SagaStore} persists.
 * <p>
 * These types carry the Axon Framework 4 API unchanged, to ease migration of projects that cannot move off it in one
 * go.
 */
@NullMarked
package org.axonframework.modelling.saga;

import org.jspecify.annotations.NullMarked;
