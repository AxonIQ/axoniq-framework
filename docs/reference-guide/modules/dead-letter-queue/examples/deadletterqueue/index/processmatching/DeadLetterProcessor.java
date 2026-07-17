package deadletterqueue.index.processmatching;

// tag::process-matching[]
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.QualifiedName;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import org.axonframework.messaging.eventhandling.EventMessage;
import java.util.concurrent.TimeUnit;

public class DeadLetterProcessor {

    private static final QualifiedName ERROR_EVENT_NAME =
            new QualifiedName("YourApplicationEvent");

    private final Configuration configuration;

    // end::process-matching[]
    DeadLetterProcessor(Configuration configuration) {
        this.configuration = configuration;
    }

    // tag::process-matching[]
    public void retryErrorEventSequence(String processorName, String componentName) {
        // dead letter processor names follow the pattern `EventHandlingComponent[" + processorName + "][" + componentName + "]`
        var dlqEhc = "EventHandlingComponent[" + processorName + "][" + componentName + "]";
        configuration.getModuleConfiguration(processorName)
                .flatMap(m -> m.getOptionalComponent(SequencedDeadLetterProcessor.class, dlqEhc))
                .ifPresent(dlp ->
                        dlp.process(letter -> ((DeadLetter<? extends EventMessage>) letter).message()
                                                    .type()
                                                    .qualifiedName()
                                                    .equals(ERROR_EVENT_NAME))
                           .orTimeout(30, TimeUnit.SECONDS)
                           .join());
    }
}
// end::process-matching[]
