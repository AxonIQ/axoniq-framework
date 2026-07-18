/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

package io.axoniq.framework.examples.faculty.write.createcourse;

import io.axoniq.framework.examples.faculty.Ids;
import io.axoniq.framework.examples.faculty.events.CourseCreated;
import io.axoniq.framework.examples.shared.ids.CourseId;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CreateCourseCommandHandlerTest {

    @Nested
    class Handle {

        @Test
        void appendsCourseCreatedEventForNewCourse() throws Exception {
            // given
            CreateCourseCommandHandler handler = new CreateCourseCommandHandler();
            CreateCourse command = new CreateCourse(CourseId.of("course-1"), "DDD", 42);
            StateFixture state = newStateFixture();
            RecordingEventAppender eventAppender = new RecordingEventAppender(state);

            // when
            handler.handle(command, state.state(), eventAppender);

            // then
            assertThat(eventAppender.events()).hasSize(1);
            assertThat(eventAppender.events().get(0)).isEqualTo(
                    new CourseCreated(Ids.FACULTY_ID, CourseId.of("course-1"), "DDD", 42)
            );
            assertThat(state.created()).isTrue();
        }

        @Test
        void throwsWhenTheSameCourseIdIsHandledTwice() throws Exception {
            // given
            CreateCourseCommandHandler handler = new CreateCourseCommandHandler();
            CreateCourse command = new CreateCourse(CourseId.of("course-1"), "DDD", 42);
            StateFixture state = newStateFixture();
            RecordingEventAppender eventAppender = new RecordingEventAppender(state);
            handler.handle(command, state.state(), eventAppender);

            // when / then
            assertThatThrownBy(() -> handler.handle(command, state.state(), eventAppender))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Course with id [Course:course-1] already exists");
            assertThat(eventAppender.events()).hasSize(1);
            assertThat(state.created()).isTrue();
        }
    }

    private static StateFixture newStateFixture() throws Exception {
        Constructor<CreateCourseCommandHandler.State> constructor =
                CreateCourseCommandHandler.State.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return new StateFixture(constructor.newInstance());
    }

    private static final class StateFixture {

        private final CreateCourseCommandHandler.State state;
        private final Method applyMethod;
        private final java.lang.reflect.Field createdField;

        private StateFixture(CreateCourseCommandHandler.State state) throws Exception {
            this.state = state;
            this.applyMethod = CreateCourseCommandHandler.State.class.getDeclaredMethod(
                    "apply",
                    CourseCreated.class
            );
            this.applyMethod.setAccessible(true);
            this.createdField = CreateCourseCommandHandler.State.class.getDeclaredField("created");
            this.createdField.setAccessible(true);
        }

        private CreateCourseCommandHandler.State state() {
            return state;
        }

        private boolean created() {
            try {
                return createdField.getBoolean(state);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to inspect state.", e);
            }
        }

        private void apply(CourseCreated event) {
            try {
                applyMethod.invoke(state, event);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to apply event to state.", e);
            }
        }
    }

    private static final class RecordingEventAppender implements EventAppender {

        private final StateFixture state;
        private final List<Object> events = new ArrayList<>();

        private RecordingEventAppender(StateFixture state) {
            this.state = state;
        }

        private List<Object> events() {
            return List.copyOf(events);
        }

        @Override
        public void append(List<?> events) {
            this.events.addAll(events);
            events.stream()
                  .filter(CourseCreated.class::isInstance)
                  .map(CourseCreated.class::cast)
                  .forEach(state::apply);
        }

        @Override
        public void append(List<?> events, Metadata metadata) {
            append(events);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("events", events.size());
        }
    }
}
