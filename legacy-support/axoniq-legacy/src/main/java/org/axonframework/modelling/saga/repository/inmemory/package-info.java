/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * An in-memory {@link org.axonframework.modelling.saga.repository.SagaStore}, for testing and for sagas whose state
 * need not outlive the JVM.
 * <p>
 * These types carry the Axon Framework 4 API unchanged, to ease migration of projects that cannot move off it in one
 * go.
 */
@NullMarked
package org.axonframework.modelling.saga.repository.inmemory;

import org.jspecify.annotations.NullMarked;
