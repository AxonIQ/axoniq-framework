package io.axoniq.framework.examples.faculty.events;

import io.axoniq.framework.examples.faculty.FacultyTags;
import io.axoniq.framework.examples.shared.ids.CourseId;
import io.axoniq.framework.examples.shared.ids.FacultyId;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record CourseCreated(
        @EventTag(key = FacultyTags.FACULTY_ID)
        FacultyId facultyId,
        @EventTag(key = FacultyTags.COURSE_ID)
        CourseId courseId,
        String name,
        int capacity
) {

}
