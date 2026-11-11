/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.util;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Test {@link MultiTenantAwareComponent} that appends every register and cancel call to an event log shared between
 * components, so tests can assert the order across components. It can be told to throw on its Nth register call or
 * Nth cancel call, and to record whether the tenant being registered is already visible through a tenant provider.
 * It is meant for single-threaded use within a test: its fields are not thread-safe, so tests read them after any
 * asynchronous work has completed. A failing register call keeps nothing of that tenant, as the
 * {@link MultiTenantAwareComponent} contract requires.
 */
public class FailingTenantAwareComponent implements MultiTenantAwareComponent {

    private final String name;
    private final List<String> eventLog;
    private final List<TenantDescriptor> registered = new ArrayList<>();
    private final List<Boolean> knownWhileRegistering = new ArrayList<>();
    private final List<Boolean> listedWhileRegistering = new ArrayList<>();
    private Consumer<TenantDescriptor> onRegister;
    private Predicate<TenantDescriptor> providerIsKnown;
    private Supplier<List<TenantDescriptor>> providerTenants;
    private int failOnRegisterNumber = -1;
    private Throwable registerFailure = new IllegalStateException("register failed");
    private String failForTenantId;
    private int failOnCancelNumber = -1;
    private Throwable cancelFailure = new IllegalStateException("cancel failed");
    private int registerCalls;
    private int cancelCalls;

    public FailingTenantAwareComponent(String name, List<String> eventLog) {
        this.name = name;
        this.eventLog = eventLog;
    }

    /**
     * Records, on every register call, whether the tenant is already known or listed by the given provider views.
     */
    public FailingTenantAwareComponent observing(Predicate<TenantDescriptor> isKnown,
                                                 Supplier<List<TenantDescriptor>> tenants) {
        this.providerIsKnown = isKnown;
        this.providerTenants = tenants;
        return this;
    }

    public FailingTenantAwareComponent failOnRegisterNumber(int number) {
        this.failOnRegisterNumber = number;
        return this;
    }

    public FailingTenantAwareComponent failOnRegisterNumber(int number, Throwable failure) {
        this.registerFailure = failure;
        return failOnRegisterNumber(number);
    }

    /**
     * Fails every register call for the tenant with the given id, for tests that cannot rely on registration order.
     */
    public FailingTenantAwareComponent failForTenant(String tenantId) {
        this.failForTenantId = tenantId;
        return this;
    }

    public FailingTenantAwareComponent failOnCancelNumber(int number) {
        this.failOnCancelNumber = number;
        return this;
    }

    public FailingTenantAwareComponent failOnCancelNumber(int number, Throwable failure) {
        this.cancelFailure = failure;
        return failOnCancelNumber(number);
    }

    public void stopFailing() {
        this.failOnRegisterNumber = -1;
        this.failForTenantId = null;
    }

    public Throwable registerFailure() {
        return registerFailure;
    }

    public Throwable cancelFailure() {
        return cancelFailure;
    }

    public List<TenantDescriptor> tenants() {
        return List.copyOf(registered);
    }

    /**
     * Per register call, whether the observed provider already reported the tenant as known.
     */
    public List<Boolean> knownWhileRegistering() {
        return List.copyOf(knownWhileRegistering);
    }

    /**
     * Per register call, whether the observed provider already listed the tenant in its tenants.
     */
    public List<Boolean> listedWhileRegistering() {
        return List.copyOf(listedWhileRegistering);
    }

    /**
     * Runs the given action on every register call, after it is logged and before the call can fail or succeed.
     */
    public FailingTenantAwareComponent onRegister(Consumer<TenantDescriptor> action) {
        this.onRegister = action;
        return this;
    }

    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        return register("register", tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        return register("registerAndStart", tenantDescriptor);
    }

    private Registration register(String method, TenantDescriptor tenant) {
        eventLog.add(method + " " + name + " " + tenant.tenantId());
        if (providerIsKnown != null) {
            knownWhileRegistering.add(providerIsKnown.test(tenant));
            listedWhileRegistering.add(providerTenants.get().contains(tenant));
        }
        if (onRegister != null) {
            onRegister.accept(tenant);
        }
        if (++registerCalls == failOnRegisterNumber || tenant.tenantId().equals(failForTenantId)) {
            throwUnchecked(registerFailure);
        }
        registered.add(tenant);
        return () -> {
            eventLog.add("cancel " + name + " " + tenant.tenantId());
            if (++cancelCalls == failOnCancelNumber) {
                throwUnchecked(cancelFailure);
            }
            return registered.remove(tenant);
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable throwable) throws T {
        throw (T) throwable;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("name", name);
    }
}
