package deadletterqueue.index;

// tag::enqueue-policy[]
import org.axonframework.messaging.core.QualifiedName;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.Decisions;
import io.axoniq.framework.messaging.deadletter.EnqueueDecision;
import io.axoniq.framework.messaging.deadletter.EnqueuePolicy;
import org.axonframework.messaging.eventhandling.EventMessage;

// ErrorEvent is your application-defined event class.
public class CustomEnqueuePolicy implements EnqueuePolicy<EventMessage> {

    private static final QualifiedName ERROR_EVENT_TYPE =
            new QualifiedName(ErrorEvent.class);

    @Override
    public EnqueueDecision<EventMessage> decide(DeadLetter<? extends EventMessage> letter,
                                                Throwable cause) {
        if (cause instanceof NullPointerException) {
            // It's pointless:
            return Decisions.doNotEnqueue();
        }


        if (letter.message().type().qualifiedName().equals(ERROR_EVENT_TYPE)) {
            // Always enqueue this:
            return Decisions.enqueue(cause);
        }

        // All others retry max 10 times:
        int retries = Integer.parseInt(letter.diagnostics().getOrDefault("retries", "-1"));
        if (retries < 10) {
            // Let's continue and increase retries:
            return Decisions.requeue(cause, l -> l.diagnostics().and("retries", String.valueOf(retries + 1)));
        }

        // Exhausted all retries:
        return Decisions.evict();
    }
}
// end::enqueue-policy[]
