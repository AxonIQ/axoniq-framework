/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.dataprotection;

import io.axoniq.framework.dataprotection.api.DataSubjectId;
import io.axoniq.framework.dataprotection.api.PersonalData;
import org.axonframework.messaging.eventhandling.annotation.Event;

@Event(namespace = "example", name = "CustomerDataRecorded", version = "1.0.0")
public record CustomerDataRecorded(
        @DataSubjectId String customerId,
        @PersonalData String email
) {

}
