package com.joshlong.mogul.api;

import com.joshlong.mogul.utils.ReflectionUtils;
import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.Set;

@Configuration
class DomainAotConfiguration {

	@Bean
	static DomainBeanFactoryInitializationAotProcessor domainHintsBeanFactoryInitializationAotProcessor() {
		return new DomainBeanFactoryInitializationAotProcessor();
	}

	static class DomainBeanFactoryInitializationAotProcessor implements BeanFactoryInitializationAotProcessor {

		@Override
		public @Nullable BeanFactoryInitializationAotContribution processAheadOfTime(
				ConfigurableListableBeanFactory beanFactory) {
			var classesSet = new HashSet<Class<?>>();
			for (var beanName : beanFactory.getBeanDefinitionNames()) {
				var type = beanFactory.getType(beanName);
				if (type != null) {
					var bases = Set.of(AbstractDomainService.class, DomainResolver.class, PublisherPlugin.class);
					for (var base : bases) {
						if (base.isAssignableFrom(type)) {
							classesSet.add(type);
							var generics = ReflectionUtils.genericsFor(type);
							classesSet.addAll(generics);
						}
					}
				}
			}

			return (generationContext, _) -> {
				var values = MemberCategory.values();
				var runtimeHints = generationContext.getRuntimeHints();
				for (var c : classesSet) {
					runtimeHints.reflection().registerType(c, values);
				}
			};
		}

	}

}
