/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Canonical serialized representation of an event association.
 *
 * @param qualifier association source qualifier, maps to {@link ValueRetriever#qualifier()}
 * @param path      source-specific path, maps to {@link ValueRetriever#path()}
 * @param operator  comparison operator name
 * @param value     serialized comparison value
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record SerializedAssociation(
        String qualifier,
        String path,
        String operator,
        String value
) {

    /**
     * Creates a serialized association from the given retriever and matcher details.
     *
     * @param retriever value retriever
     * @param operator  comparison operator name
     * @param value     comparison value
     * @return serialized association
     */
    public static SerializedAssociation from(
            ValueRetriever retriever,
            String operator,
            Object value
    ) {
        Objects.requireNonNull(retriever, "Value retriever is mandatory");
        Objects.requireNonNull(operator, "Operator is mandatory");
        Objects.requireNonNull(value, "Association value is mandatory");
        return new SerializedAssociation(
                retriever.qualifier(),
                retriever.path(),
                operator,
                String.valueOf(value)
        );
    }

    /**
     * Parses a serialized association string.
     *
     * @param registry            operator registry used to resolve operators
     * @param serializedCondition serialized association
     * @return parsed serialized association
     */
    public static SerializedAssociation parse(
            ValueComparisonOperatorRegistry registry,
            String serializedCondition
    ) {
        Objects.requireNonNull(registry, "Registry must not be null");
        Objects.requireNonNull(serializedCondition, "Association string must not be null");

        var operatorNames = registry.getOperatorNames();
        var operator = operatorNames.stream()
                                    .sorted(Comparator.comparingInt(String::length).reversed())
                                    .filter(serializedCondition::contains)
                                    .findFirst()
                                    .orElseThrow(() -> BadAssociationFormatException.unsupportedOperator(
                                            operatorNames,
                                            serializedCondition
                                    ));

        var operatorIndex = serializedCondition.indexOf(operator);
        if (operatorIndex < 1 || operatorIndex + operator.length() >= serializedCondition.length()) {
            throw BadAssociationFormatException.wrongFormat(serializedCondition);
        }

        var left = serializedCondition.substring(0, operatorIndex);
        var right = serializedCondition.substring(operatorIndex + operator.length());
        if (left.isBlank() || right.isBlank()) {
            throw BadAssociationFormatException.wrongFormat(serializedCondition);
        }

        var qualifierSeparator = left.indexOf(':');
        if (qualifierSeparator < 1 || qualifierSeparator == left.length() - 1) {
            throw BadAssociationFormatException.wrongFormat(serializedCondition);
        }
        String qualifier = left.substring(0, qualifierSeparator);
        String path = left.substring(qualifierSeparator + 1);
        if (qualifier.isBlank() || path.isBlank()) {
            throw BadAssociationFormatException.wrongFormat(serializedCondition);
        }
        if (!supportedQualifiers().contains(qualifier)) {
            throw BadAssociationFormatException.unsupportedQualifier(supportedQualifiers(), serializedCondition);
        }

        return new SerializedAssociation(qualifier, path, operator, right);
    }

    private static Object serializeRetrievedValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Set<String> supportedQualifiers() {
        return Set.of(PayloadPropertyValueRetriever.QUALIFIER, MetadataPropertyValueRetriever.QUALIFIER);
    }

    /**
     * Delivers a predicate that can be used to filter event messages.
     *
     * @param registry value comparison operator registry
     * @return bi predicate that can be used to filter event messages
     */
    public BiPredicate<EventMessage, ProcessingContext> asEventMessagePredicate(
            ValueComparisonOperatorRegistry registry
    ) {
        Objects.requireNonNull(registry, "Registry must not be null");
        var retriever = valueRetriever();
        var comparisonOperator = registry.get(operator);
        return (eventMessage, processingContext) -> {
            Object retrievedValue = retriever.apply(eventMessage, processingContext);
            return comparisonOperator.apply(value, serializeRetrievedValue(retrievedValue));
        };
    }

    /**
     * Serializes the association.
     *
     * @return serialized association
     */
    public String serialize() {
        return qualifier + ":" + path + operator + value;
    }

    @Override
    public String toString() {
        return serialize();
    }

    private ValueRetriever valueRetriever() {
        return switch (qualifier) {
            case PayloadPropertyValueRetriever.QUALIFIER -> PayloadPropertyValueRetriever.payloadProperty(path);
            case MetadataPropertyValueRetriever.QUALIFIER -> MetadataPropertyValueRetriever.metadataProperty(path);
            default -> throw BadAssociationFormatException.unsupportedQualifier(supportedQualifiers(), serialize());
        };
    }
}
