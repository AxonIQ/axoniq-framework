package io.axoniq.framework.examples.faculty.write.createcourse;

import io.axoniq.framework.examples.shared.ids.CourseId;
import org.axonframework.messaging.commandhandling.annotation.Command;

@Command
public record CreateCourse(CourseId courseId, String name, int capacity) {

}
