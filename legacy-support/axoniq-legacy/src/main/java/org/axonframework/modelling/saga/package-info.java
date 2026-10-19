/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
