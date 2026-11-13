/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantqueryhandling;

import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;

public class CourseStatisticsQueryHandler {

    // tag::tenant-scoped-query-handler[]
    @QueryHandler
    public CourseStatistics on(FindCourseStatistics query, @TenantScoped CourseStatisticsRepository repository) { // <1>
        return repository.findById(query.courseId());
    }
    // end::tenant-scoped-query-handler[]
}
