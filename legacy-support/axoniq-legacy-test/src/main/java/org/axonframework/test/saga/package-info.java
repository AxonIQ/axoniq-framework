/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Test support for the sagas carried by {@code axon-legacy}.
 * <p>
 * The package name matches Axon Framework 4, so a migrating saga test suite keeps its imports. What it contains is a
 * thin layer over {@link org.axonframework.test.fixture.AxonTestFixture}: the given-when-then flow, the recording of
 * published events and dispatched commands, and the configuration all come from there, while this package adds the
 * saga-specific pieces, such as asserting on the contents of a
 * {@link org.axonframework.modelling.saga.repository.SagaStore}.
 * <p>
 * A test that does not need the Axon Framework 4 API can use {@code AxonTestFixture} directly and reach for the
 * assertion helpers here through its {@code then().expect(..)} operation.
 */
@NullMarked
package org.axonframework.test.saga;

import org.jspecify.annotations.NullMarked;
