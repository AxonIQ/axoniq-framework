/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */


/**
 * Part of the Axoniq Framework multi-tenancy module. Contains the Axon Server counterparts of the factories in
 * {@link io.axoniq.framework.messaging.multitenancy.eventsourcing}, providing each tenant its event storage engine
 * and snapshot store on that tenant's own connection.
 */
@NullMarked
package io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing;

import org.jspecify.annotations.NullMarked;
