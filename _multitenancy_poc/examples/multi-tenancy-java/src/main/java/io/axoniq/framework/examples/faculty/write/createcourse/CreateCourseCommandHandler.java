package io.axoniq.framework.examples.faculty.write.createcourse;

import io.axoniq.framework.examples.faculty.FacultyTags;
import io.axoniq.framework.examples.faculty.Ids;
import io.axoniq.framework.examples.faculty.events.CourseCreated;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;

import java.util.List;

class CreateCourseCommandHandler {

    @CommandHandler
    void handle(
            CreateCourse command,
            @InjectEntity(idProperty = FacultyTags.COURSE_ID) State state,
            EventAppender eventAppender
    ) {
        if (state.created) {
            throw new IllegalStateException(
                    "Course with id [%s] already exists".formatted(command.courseId())
            );
        }
        eventAppender.append(List.of(new CourseCreated(
                Ids.FACULTY_ID,
                command.courseId(),
                command.name(),
                command.capacity()
        )));
    }

    @EventSourcedEntity(tagKey = FacultyTags.COURSE_ID)
    static final class State {

        private boolean created;

        @EntityCreator
        private State() {
            this.created = false;
        }

        @EventSourcingHandler
        private State apply(CourseCreated event) {
            this.created = true;
            return this;
        }
    }
}
