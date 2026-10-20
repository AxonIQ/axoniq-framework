/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Test support for the deadlines carried by {@code axoniq-legacy}.
 * <p>
 * The package name matches Axon Framework 4, so a migrating test suite keeps its imports. The
 * {@link org.axonframework.test.deadline.StubDeadlineManager} records what is scheduled and fires deadlines when a
 * test advances its time, and the {@link org.axonframework.test.deadline.DeadlineManagerValidator} asserts on what it
 * recorded. The {@link org.axonframework.test.saga.SagaTestFixture} uses both for its deadline assertions.
 */
@NullMarked
package org.axonframework.test.deadline;

import org.jspecify.annotations.NullMarked;
