package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.Settings;
import com.joshlong.podbean.token.ClientCredentialsTokenProvider;
import com.joshlong.podbean.token.Token;
import com.joshlong.podbean.token.TokenProvider;
import org.aopalliance.intercept.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.util.Assert;

/**
 * By default, the Podbean autoconfiguration creates a global and singular instance of
 * {@link TokenProvider tp} based on the configuration stipulated at design time.
 * <p>
 * This is a multi-tenant implementation that is aware of the currently signed in
 * {@link Mogul }.
 */
@Configuration
@ImportRuntimeHints(PodbeanConfiguration.Hints.class)
class PodbeanConfiguration {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			var clazzes = new Class<?>[] { com.joshlong.podbean.token.TokenProvider.class,
					org.springframework.aop.SpringProxy.class, org.springframework.aop.framework.Advised.class,
					org.springframework.core.DecoratingProxy.class };
			var mcs = MemberCategory.values();
			for (var c : clazzes)
				hints.reflection().registerType(TypeReference.of(c), mcs);
			hints.proxies().registerJdkProxy(clazzes);
		}

	}

	@Bean
	TokenProvider multitenantTokenProvider(MogulService mogulService, Settings settings) {
		var tenantAwareTokenProvider = new MogulAwareTokenProvider(mogulService, settings);

		var tokenProviderClass = TokenProvider.class;
		var proxyFactory = new ProxyFactory();
		proxyFactory.setTargetClass(tokenProviderClass);
		proxyFactory.setInterfaces(tokenProviderClass.getInterfaces());
		proxyFactory.setProxyTargetClass(true);
		proxyFactory.addAdvice((MethodInterceptor) invocation -> {

			if (!invocation.getMethod().getName().equals("getToken"))
				return invocation.proceed();

			return tenantAwareTokenProvider.getToken();

		});
		return (TokenProvider) proxyFactory.getProxy();
	}

	static class MogulAwareTokenProvider implements TokenProvider {

		private final Logger log = LoggerFactory.getLogger(getClass());

		private final MogulService mogulService;

		private final Settings settings;

		MogulAwareTokenProvider(MogulService mogulService, Settings settings) {
			this.mogulService = mogulService;
			this.settings = settings;
		}

		@Override
		public Token getToken() {
			var currentMogul = mogulService.getCurrentMogul();
			var mogulId = currentMogul.id();
			var settingsForTenant = settings.getAllSettingsByCategory(mogulId,
					PodbeanPodcastEpisodePublisherPlugin.PLUGIN_NAME);
			var clientId = settingsForTenant.get("clientId").value();
			var clientSecret = settingsForTenant.get("clientSecret").value();
			Assert.hasText(clientId, "the podbean clientId for mogul [" + mogulId + "] is empty");
			Assert.hasText(clientSecret, "the podbean clientSecret for mogul [" + mogulId + "] is empty");
			log.debug("returning podbean {} for mogul [{}]", ClientCredentialsTokenProvider.class.getName(),
					currentMogul.username());
			return new ClientCredentialsTokenProvider(clientId, clientSecret).getToken();
		}

	}

}
