/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Spring Boot autoconfiguration for the Axon Framework 4 Sagas carried by {@code axon-legacy}. Activates the saga
 * store and saga processor wiring when the matching infrastructure (a {@code DataSource}, an
 * {@code EntityManagerFactory}, or neither) is available.
 */
@NullMarked
package org.axonframework.extension.springboot.autoconfig;

import org.jspecify.annotations.NullMarked;
