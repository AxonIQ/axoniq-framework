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

package org.axonframework.integrationtests.testsuite.student.state;

import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.integrationtests.testsuite.student.events.MentorAssignedToStudentEvent;
import org.axonframework.integrationtests.testsuite.student.events.StudentEnrolledEvent;
import org.axonframework.integrationtests.testsuite.student.events.StudentNameChangedEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Event-sourced Student entity
 */
public class Student {

    private String id;
    private String name;
    private String mentorId;
    private String menteeId;
    private List<String> coursesEnrolled = new ArrayList<>();

    public Student(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getCoursesEnrolled() {
        return coursesEnrolled;
    }

    public String getMentorId() {
        return mentorId;
    }

    public String getMenteeId() {
        return menteeId;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @EventSourcingHandler
    public void handle(StudentEnrolledEvent event) {
        coursesEnrolled.add(event.courseId());
    }

    @EventSourcingHandler
    public void handle(StudentNameChangedEvent event) {
        name = event.name();
    }

    @EventSourcingHandler
    public void handle(MentorAssignedToStudentEvent event) {
        if (event.mentorId().equals(this.id)) {
            // I have been assigned a mentee!
            this.menteeId = event.menteeId();
        } else if (event.menteeId().equals(this.id)) {
            // I have been assigned a mentor!
            this.mentorId = event.mentorId();
        }
    }
}
