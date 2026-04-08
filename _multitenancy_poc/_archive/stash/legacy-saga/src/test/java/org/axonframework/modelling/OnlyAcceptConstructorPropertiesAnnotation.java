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

package org.axonframework.modelling;

import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;

import java.beans.ConstructorProperties;

/**
 * Internal {@link AnnotationIntrospector} for jackson to test JSON conversion ignoring all creator annotation
 * except for the {@link ConstructorProperties}-annotation.
 *
 * @author JohT
 */
public class OnlyAcceptConstructorPropertiesAnnotation extends JacksonAnnotationIntrospector {

    public static final ObjectMapper attachTo(ObjectMapper objectMapper) {
        return objectMapper.setAnnotationIntrospector(new OnlyAcceptConstructorPropertiesAnnotation());
    }

    @Override
    public Mode findCreatorAnnotation(MapperConfig<?> config, Annotated annotated) {
        return (annotated.hasAnnotation(ConstructorProperties.class)) ? super.findCreatorAnnotation(config,
                                                                                                    annotated) : Mode.DISABLED;
    }
}