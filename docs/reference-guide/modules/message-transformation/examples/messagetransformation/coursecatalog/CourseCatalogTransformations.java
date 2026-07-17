package messagetransformation.coursecatalog;

// tag::register-chain[]
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;

public final class CourseCatalogTransformations {

    public static EventTransformerChain chain() {
        return EventTransformerChain.builder()                               // <1>
                                    .register(SystemAnnouncementLegacyUplift.build())
                                    .register(CoursePublishedV1ToV2.build()) // <2>
                                    .register(CoursePublishedV2ToV3.build())
                                    .register(StudentRegisteredV1ToV2.build())
                                    .register(WelcomeMessageBetaCleanup.build())
                                    .register(SystemHeartbeatDrop.build())   // <3>
                                    .build();                                // <4>
    }
}
// end::register-chain[]
