/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantscopedcomponents;

import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;

public class CourseStatisticsProjection {

    // tag::inject-into-handler[]
    @EventHandler
    public void on(CourseCreated event, @TenantScoped CourseStatisticsStore store) { // <1>
        store.save(new CourseStatistics(event.courseId()));
    }
    // end::inject-into-handler[]
}
