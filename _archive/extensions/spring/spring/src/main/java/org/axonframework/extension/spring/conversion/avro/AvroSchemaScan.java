/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package org.axonframework.extension.spring.conversion.avro;

import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Configures the base packages used by autoconfiguration when scanning for classes containing Avro Schema. One of
 * {@link #basePackageClasses()}, {@link #basePackages()} or its alias {@link #value()} may be specified to define
 * specific packages to scan. If specific packages are not defined scanning will occur from the package of the class
 * with this annotation.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 * @see AvroSchemaPackages
 */
@Retention(RetentionPolicy.RUNTIME)
@Import(AvroSchemaPackages.Registrar.class)
@Target(ElementType.TYPE)
@Documented
@Inherited
public @interface AvroSchemaScan {

    /**
     * Base packages to scan for classes containing Avro Schemas. {@link #value()} is an alias for (and mutually
     * exclusive with) this attribute.
     * <p>
     * Use {@link #basePackageClasses()} for a type-safe alternative to String-based package names.
     *
     * @return the base packages to scan
     */
    String[] basePackages() default {};

    /**
     * Type-safe alternative to {@link #basePackages()} for specifying the packages to scan for classes containing Avro
     * Schemas. The package of each class specified will be scanned.
     * <p>
     * Consider creating a special no-op marker class or interface in each package that serves no purpose other than
     * being referenced by this attribute.
     *
     * @return classes from the base packages to scan
     */
    Class<?>[] basePackageClasses() default {};

    /**
     * Alias for the {@link #basePackages()} attribute. Allows for more concise annotation declarations e.g.:
     * {@code @AvroSchemaScan("org.my.pkg")} instead of {@code @AvroSchemaScan(basePackages="org.my.pkg")}.
     *
     * @return the base packages to scan
     */
    @AliasFor("basePackages")
    String[] value() default {};
}
