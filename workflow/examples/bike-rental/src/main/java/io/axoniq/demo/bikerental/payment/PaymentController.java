package io.axoniq.demo.bikerental.payment;

import io.axoniq.demo.bikerental.coreapi.payment.ConfirmPaymentCommand;
import io.axoniq.demo.bikerental.coreapi.payment.GetAllPaymentsQuery;
import io.axoniq.demo.bikerental.coreapi.payment.GetPaymentIdQuery;
import io.axoniq.demo.bikerental.coreapi.payment.GetPaymentStatusQuery;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus;
import io.axoniq.demo.bikerental.coreapi.payment.RejectPaymentCommand;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
public class PaymentController {

    private final QueryGateway queryGateway;
    private final CommandGateway commandGateway;

    public PaymentController(QueryGateway queryGateway, CommandGateway commandGateway) {
        this.queryGateway = queryGateway;
        this.commandGateway = commandGateway;
    }

    @GetMapping("/status/{paymentId}")
    public CompletableFuture<PaymentStatus> getStatus(@PathVariable("paymentId") String paymentId) {
        return queryGateway.query(new GetPaymentStatusQuery(paymentId), PaymentStatus.class);
    }

    @GetMapping("/findPayment")
    public CompletableFuture<String> findPaymentId(@RequestParam("reference") String paymentReference) {
        return queryGateway.query(new GetPaymentIdQuery(paymentReference), String.class);
    }

    @PostMapping("/acceptPayment")
    public CompletableFuture<Void> confirmPayment(@RequestParam("id") String paymentId) {
        return commandGateway.send(new ConfirmPaymentCommand(paymentId)).resultAs(Void.class);
    }

    @PostMapping("/rejectPayment")
    public CompletableFuture<Void> rejectPayment(@RequestParam("id") String paymentId) {
        return commandGateway.send(new RejectPaymentCommand(paymentId)).resultAs(Void.class);
    }

    @GetMapping("/status")
    public CompletableFuture<List<PaymentStatus>> getStatus(
            @RequestParam(value = "status", required = false) PaymentStatus.Status status
    ) {
        return queryGateway.queryMany(new GetAllPaymentsQuery(status), PaymentStatus.class);
    }
}
