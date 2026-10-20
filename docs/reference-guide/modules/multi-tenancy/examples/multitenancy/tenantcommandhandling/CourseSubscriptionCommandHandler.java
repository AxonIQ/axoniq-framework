/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantcommandhandling;

import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

public class CourseSubscriptionCommandHandler {

    // tag::tenant-scoped-command-handler[]
    @CommandHandler
    public void on(NotifyCourseSubscription command,
                    @TenantScoped NotificationService notificationService,
                    EventAppender appender) { // <1>
        notificationService.notifyStudentCourseCreation(command.studentId(), command.courseId());
        appender.append(new CourseSubscriptionNotified(command.courseId(), command.studentId()));
    }
    // end::tenant-scoped-command-handler[]
}
