/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
/**
 * A {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex} keeping the workflow engine's index of running
 * workflows durable in a relational database, so a restarted application restores its segments from that index instead
 * of reading every workflow lifecycle event, together with the entities it stores the index in.
 * <p>
 * Requires {@code jakarta.persistence-api} and a JPA provider on the classpath; the workflow engine declares them as
 * optional dependencies.
 *
 * @since 5.5.0
 */
@NullMarked
package io.axoniq.framework.workflow.index.jpa;

import org.jspecify.annotations.NullMarked;
