/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenanteventprocessing;

import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

public class EnrollmentProjection {

    // tag::tenant-scoped-event-handler[]
    @EventHandler
    public void on(StudentEnrolled event, @TenantScoped EnrollmentRepository repository) { // <1>
        repository.save(new Enrollment(event.studentId(), event.courseId()));
    }
    // end::tenant-scoped-event-handler[]
}
