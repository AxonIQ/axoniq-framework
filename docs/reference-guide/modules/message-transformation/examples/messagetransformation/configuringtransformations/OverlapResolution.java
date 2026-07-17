package messagetransformation.configuringtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import messagetransformation.coursecatalog.WelcomeMessage090ToV1;
import messagetransformation.coursecatalog.WelcomeMessageBetaCleanup;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import tools.jackson.databind.JsonNode;

final class OverlapResolution {

    private OverlapResolution() {
    }

    static void register() {
        // tag::matching-precedence[]
        QualifiedName welcomeMessageSent = new QualifiedName("coursecatalog.WelcomeMessageSent");
        MessageType v1 = new MessageType(welcomeMessageSent, "1.0.0");

        EventTransformation betaCleanup =                                       // <1>
                EventTransformation.from(type -> type.version().startsWith("0."))
                                   .declaringFromTypes(welcomeMessageSent)
                                   .to(v1)
                                   .transform(JsonNode.class, WelcomeMessageBetaCleanup::map);

        EventTransformation beta090ToV1 =                                       // <2>
                EventTransformation.from(new MessageType(welcomeMessageSent, "0.9.0"))
                                   .to(v1)
                                   .transform(JsonNode.class, WelcomeMessage090ToV1::map);

        EventTransformerChain.builder()
                             .register(betaCleanup)
                             .register(beta090ToV1)
                             .build();
        // end::matching-precedence[]
    }
}
