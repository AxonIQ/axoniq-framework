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

package org.axonframework.common.jpa;

import jakarta.persistence.EntityManager;
import org.axonframework.common.annotation.Internal;

/**
 * Provides components with an EntityManager to access the persistence mechanism. Depending on the application
 * environment, this may be a single container managed EntityManager, or an application managed instance for one-time
 * use.
 * <p/>
 * Note that the implementation is responsible for keeping track of transaction scope, if necessary. Generally, this is
 * the case when using application-managed EntityManagers.
 *
 * @author Allard Buijze
 * @since 1.3
 */
@Internal
public interface EntityManagerProvider {

    /**
     * Returns the EntityManager instance to use.
     * <p/>
     * Note that the implementation is responsible for keeping track of transaction scope, if necessary. Generally,
     * this is the case when using application-managed EntityManagers.
     *
     * @return the EntityManager instance to use.
     */
    EntityManager getEntityManager();
}
