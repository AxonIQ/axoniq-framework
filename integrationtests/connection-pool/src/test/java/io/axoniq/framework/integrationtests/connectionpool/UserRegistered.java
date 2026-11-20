/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import org.axonframework.eventsourcing.annotation.EventTag;

/**
 * A simple tagged event used by all scenarios.
 *
 * @param userId the identifier of the user, used as tag
 * @param email  the email address of the user
 * @author John Hendrikx
 */
record UserRegistered(@EventTag(key = "user") String userId, String email) {
}
