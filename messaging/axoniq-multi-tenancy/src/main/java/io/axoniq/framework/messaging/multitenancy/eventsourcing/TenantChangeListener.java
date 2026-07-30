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
package io.axoniq.framework.messaging.multitenancy.eventsourcing;

import org.axonframework.common.annotation.Internal;

/**
 * Listener notified whenever the tenants a component holds change.
 * <p>
 * A component announces a change only once it is visible through its own tenants, so a listener reading them acts on
 * what the announcing component actually holds. That is the difference with following a
 * {@link io.axoniq.framework.messaging.multitenancy.api.TenantProvider TenantProvider}
 * directly, whose subscribers are notified in turn, so one ahead of the component may still be registering when the
 * notification arrives.
 * <p>
 * Invoked on the thread applying the change, so an implementation must return promptly and hand off any work of its
 * own.
 * <p>
 * Internal, because the only component announcing to one is internal itself.
 *
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
@FunctionalInterface
public interface TenantChangeListener {

    /**
     * Invoked after the tenants of the announcing component changed.
     */
    void onTenantsChanged();
}
