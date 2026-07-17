package deadletterqueue.index.processall;

// tag::process-all[]
import org.axonframework.common.configuration.Configuration;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import java.util.concurrent.TimeUnit;

public class DeadLetterProcessor {

    private final Configuration configuration;

    // end::process-all[]
    DeadLetterProcessor(Configuration configuration) {
        this.configuration = configuration;
    }

    // tag::process-all[]
    public void retryAllSequences(String processorName, String componentName) {
        // dead letter processor names follow the pattern `EventHandlingComponent[" + processorName + "][" + componentName + "]`
        var dlqEhc = "EventHandlingComponent[" + processorName + "][" + componentName + "]";
        configuration.getModuleConfiguration(processorName)
                .flatMap(m -> m.getOptionalComponent(SequencedDeadLetterProcessor.class, dlqEhc))
                .ifPresent(this::processUntilEmpty);
    }

    private void processUntilEmpty(SequencedDeadLetterProcessor<?> dlp) {
        boolean processed = true;
        while (processed) {
            processed = dlp.processAny()
                           .orTimeout(30, TimeUnit.SECONDS)
                           .join();
        }
    }
}
// end::process-all[]
