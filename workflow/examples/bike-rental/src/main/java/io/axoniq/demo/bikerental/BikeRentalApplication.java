package io.axoniq.demo.bikerental;

import io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus;
import io.axoniq.demo.bikerental.coreapi.rental.BikeStatus;
import io.axoniq.demo.bikerental.rental.RentalApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@EntityScan(basePackageClasses = {PaymentStatus.class, BikeStatus.class})
@SpringBootApplication
public class BikeRentalApplication {

    public static void main(String[] args) {
        SpringApplication.run(RentalApplication.class, args);
    }

    /**
     *        @Autowired
     *    public void configure(EventProcessingConfigurer config) {
     * 		config.registerPooledStreamingEventProcessor(
     * 				"io.axoniq.demo.bikerental.payment",
     * 				Configuration::eventStore,
     * 				(c, b) -> b.workerExecutor(workerExecutorService())
     * 						   .batchSize(100)
     * 		);
     *    }
     */

    /**
     *
     @Autowired public void configure(EventProcessingConfigurer eventProcessing) {
     eventProcessing.registerPooledStreamingEventProcessor(
     "PaymentSagaProcessor",
     Configuration::eventStore,
     (c, b) -> b.workerExecutor(workerExecutorService())
     .batchSize(100)
     .initialToken(StreamableMessageSource::createHeadToken)
     );
     eventProcessing.registerPooledStreamingEventProcessor(
     "io.axoniq.demo.bikerental.rental.query",
     Configuration::eventStore,
     (c, b) -> b.workerExecutor(workerExecutorService())
     .batchSize(100)

     );
     }

     */

}
