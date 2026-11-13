/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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