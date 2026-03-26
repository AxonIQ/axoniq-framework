package io.axoniq.workflow.springboot;

import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import jakarta.annotation.Nonnull;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class WorkflowDefinitionLookup implements BeanDefinitionRegistryPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionLookup.class);

    @Override
    public void postProcessBeanFactory(@Nonnull ConfigurableListableBeanFactory beanFactory) throws BeansException {
        if (!(beanFactory instanceof BeanDefinitionRegistry)) {
            logger.warn("Given bean factory is not a BeanDefinitionRegistry. Cannot auto-configure workflow handlers");
            return;
        }

        String configurerBeanName = "WorkflowHandlerConfigurer$$Axon$$WorkflowDefinition";
        if (beanFactory.containsBeanDefinition(configurerBeanName)) {
            logger.info("Workflow handler configurer already available. Skipping configuration");
            return;
        }

        List<String> found = handlerBeans(WorkflowContext.class, beanFactory);
        if (!found.isEmpty()) {
            List<String> sortedFound = sortByOrder(found, beanFactory);
            AbstractBeanDefinition beanDefinition =
                    BeanDefinitionBuilder.genericBeanDefinition(MessageHandlerConfigurer.class)
                                         .addConstructorArgValue(value.name())
                                         .addConstructorArgValue(sortedFound)
                                         .getBeanDefinition();
            ((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(configurerBeanName, beanDefinition);
        }
    }


    /**
     * Returns a list of beans found in the given {@code register} that contain a handler for the given
     * {@code workflowContextType}. The search will only consider prototype beans (or any other non-singleton or
     * abstract bean definitions) when {@code includePrototypeBeans} is {@code true}.
     *
     * @param workflowContextType   The type of workflow to find handlers for.
     * @param registry              The registry to find these handlers in.
     * @param includePrototypeBeans Whether to include prototype beans.
     * @return A list of bean names with message handlers.
     */
    public static List<String> handlerBeans(Class<?> workflowContextType,
                                            ConfigurableListableBeanFactory registry,
                                            boolean includePrototypeBeans) {
        List<String> found = new ArrayList<>();
        for (String beanName : registry.getBeanDefinitionNames()) {
            BeanDefinition bd = registry.getBeanDefinition(beanName);

            if (bd.isAutowireCandidate()) {  // excludes unproxied variants of proxied beans
                if (includePrototypeBeans || (bd.isSingleton() && !bd.isAbstract())) {
                    Class<?> beanType = registry.getType(beanName);
                    if (beanType != null && hasWorkflowHandlerHandler(workflowContextType, beanType)) {
                        found.add(beanName);
                    }
                }
            }
        }
        return found;
    }

    public static boolean hasWorkflowHandlerHandler(Class<?> workflowContextType, Class<?> beanType) {
        for (Method m : ReflectionUtils.methodsOf(beanType)) {
            Optional<Map<String, Object>> attr = AnnotationUtils.findAnnotationAttributes(m, Workflow.class);
            if (attr.isPresent() && workflowContextType.isAssignableFrom((Class<?>) attr.get().get("workflowContextType"))) {
                return true;
            }
        }
        return false;
    }
}
