package io.axoniq.framework.examples.faculty.write.createcourse;

import io.axoniq.framework.examples.shared.ids.CourseId;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;

public enum CreateCourseConfiguration {
    ;

    public static EventSourcingConfigurer configure(EventSourcingConfigurer configurer) {
        var stateEntity = EventSourcedEntityModule
                .autodetected(CourseId.class, CreateCourseCommandHandler.State.class);

        var commandHandlingModule = CommandHandlingModule
                .named("CreateCourse")
                .commandHandlers()
                .autodetectedCommandHandlingComponent(c -> new CreateCourseCommandHandler());

//        var courseNameUniqueNameSetValidation = EventProcessorModule
//                .subscribing("CourseNameUniqueNameSetValidation")
//                .eventHandlingComponents(eh -> eh.autodetected(cfg -> new CourseUniqueNameSetValidation()))
//                .notCustomized();

        return configurer
                .registerEntity(stateEntity)
                .registerCommandHandlingModule(commandHandlingModule)
                ;
//                .messaging(ms -> ms.eventProcessing(ep -> ep.subscribing(s -> s.processor(courseNameUniqueNameSetValidation))));
    }
}
