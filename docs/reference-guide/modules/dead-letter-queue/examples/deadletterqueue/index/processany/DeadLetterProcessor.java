package deadletterqueue.index.processany;

// tag::process-any[]
import org.axonframework.common.configuration.Configuration;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import java.util.concurrent.TimeUnit;

public class DeadLetterProcessor {

    private final Configuration configuration;

    // end::process-any[]
    DeadLetterProcessor(Configuration configuration) {
        this.configuration = configuration;
    }

    // tag::process-any[]
    public void retryAnySequence(String processorName, String componentName) {
        // dead letter processor names follow the pattern `EventHandlingComponent[" + processorName + "][" + componentName + "]`
        var dlqEhc = "EventHandlingComponent[" + processorName + "][" + componentName + "]";
        configuration.getModuleConfiguration(processorName)
                .flatMap(m -> m.getOptionalComponent(SequencedDeadLetterProcessor.class, dlqEhc))
                .ifPresent(dlp ->
                        dlp.processAny()
                           .orTimeout(30, TimeUnit.SECONDS)
                           .join());
    }
}
// end::process-any[]
