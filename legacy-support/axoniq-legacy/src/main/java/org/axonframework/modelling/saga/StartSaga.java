/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.modelling.saga;

import org.axonframework.messaging.core.annotation.HasHandlerAttributes;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated {@link SagaEventHandler} method can trigger the creation of a new Saga instance.
 * <p/>
 * When a Saga is started due to an invocation on a StartSaga annotated method, the association of the annotated method
 * and the actual property's value are used to define a AssociationValue for the created saga. Thus, a method with this
 * definition:
 * <p/>
 * <code>@StartSaga(forceNew=true)<br/>@SagaEventHandler(associationProperty=&quot;orderId&quot;)<br/>public void
 * handleOrderCreated(OrderCreatedEvent event) </code><br/> will always trigger the creation of a saga that can be found
 * with an AssociationValue with key "orderId" and as value the value returned by {@code event.getOrderId()}.
 * <p/>
 * This annotation can only appear on methods that have been annotated with {@link SagaEventHandler @SagaEventHandler}.
 *
 * @author Allard Buijze
 * @since 0.7
 * @deprecated The {@code axon-legacy} module exists to let already-running Axon Framework 4 Sagas drain out, not to
 * start new ones. Model new business processes as a Workflow or a Stateful Event Handler instead. This annotation
 * remains functional so already-running sagas keep starting related saga instances correctly, but declaring it on new
 * saga types is discouraged.
 */
@Deprecated(since = "5.4.0", forRemoval = true)
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@HasHandlerAttributes
public @interface StartSaga {

    /**
     * Indicates whether or not to force creation of a Saga, even if one already exists. If {@code true}, a new Saga is
     * always created when an event assignable to the annotated method is handled. If {@code false}, a new saga is only
     * created if no Saga's exist that can handle the incoming event.
     * <p/>
     * This annotation can only appear on methods that have been annotated with
     * {@link SagaEventHandler @SagaEventHandler}.
     */
    boolean forceNew() default false;
}
