/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Test support for the event schedulers carried by {@code axoniq-legacy}.
 * <p>
 * The package name matches Axon Framework 4, so a migrating test suite keeps its imports. The
 * {@link org.axonframework.test.eventscheduler.StubEventScheduler} records what is scheduled and hands scheduled events
 * out when a test advances its time, and the {@link org.axonframework.test.eventscheduler.EventSchedulerValidator}
 * asserts on what it recorded. The {@link org.axonframework.test.saga.SagaTestFixture} uses both for its scheduled
 * event assertions.
 */
@NullMarked
package org.axonframework.test.eventscheduler;

import org.jspecify.annotations.NullMarked;
