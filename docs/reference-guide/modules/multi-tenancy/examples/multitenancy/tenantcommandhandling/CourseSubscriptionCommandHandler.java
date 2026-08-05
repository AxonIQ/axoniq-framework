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
