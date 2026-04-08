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
import org.axonframework.eventsourcing.annotation.EventCriteriaBuilder;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.integrationtests.testsuite.student.common.StudentMentorModelIdentifier;
import org.axonframework.integrationtests.testsuite.student.events.MentorAssignedToStudentEvent;

import java.util.List;

@EventSourcedEntity
public class StudentMentorAssignment {

    private StudentMentorModelIdentifier identifier;
    private boolean mentorHasMentee;
    private boolean menteeHasMentor;

    public StudentMentorAssignment(
            StudentMentorModelIdentifier identifier) {
        this.identifier = identifier;
    }

    public boolean isMentorHasMentee() {
        return mentorHasMentee;
    }

    public boolean isMenteeHasMentor() {
        return menteeHasMentor;
    }

    @EventSourcingHandler
    public void handle(MentorAssignedToStudentEvent event) {
        if (event.mentorId().equals(this.identifier.mentorId())) {
            mentorHasMentee = true;
        } else if (event.menteeId().equals(this.identifier.menteeId())) {
            menteeHasMentor = true;
        }
    }

    @EventCriteriaBuilder
    public static EventCriteria resolveCriteria(StudentMentorModelIdentifier id) {
        return EventCriteria.either(
                List.of(
                        EventCriteria.havingTags(new Tag("Student", id.menteeId()))
                                     .andBeingOneOfTypes(MentorAssignedToStudentEvent.class.getName()),
                        EventCriteria.havingTags(new Tag("Student", id.mentorId()))
                                     .andBeingOneOfTypes(MentorAssignedToStudentEvent.class.getName())
                )
        );
    }
}
