package io.axoniq.workflow.runtime.engine.association;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AssociationValueTest {

    @Test
    void applyEqualsComparison() {
        EqualsComparison equalsComparison = new EqualsComparison();
        AssociationValue associationValue = new AssociationValue("key", equalsComparison, "expected");

        assertTrue(associationValue.apply("expected"));
        assertFalse(associationValue.apply("actual"));
        assertFalse(associationValue.apply(null));
    }

    @Test
    void applyEqualsComparisonWithNull() {
        EqualsComparison equalsComparison = new EqualsComparison();
        AssociationValue associationValue = new AssociationValue("key", equalsComparison, "something");
        // Check if null is handled correctly when actual value is null
        assertFalse(associationValue.apply(null));

        // When both are null
        // AssociationValue constructor has @Nonnull for associationValue, but let's see if it's enforced at runtime
        // In the EqualsComparison.apply implementation we handle nulls.
    }
}
