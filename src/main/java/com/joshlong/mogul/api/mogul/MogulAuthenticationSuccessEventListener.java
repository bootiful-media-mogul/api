package com.joshlong.mogul.api.mogul;

import org.springframework.context.ApplicationListener;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.stereotype.Component;

/**
 * invoked immediately after the first OAuth dance redirects to the client which then
 * proxies to this resource server
 */
@Component
class MogulAuthenticationSuccessEventListener implements ApplicationListener<AuthenticationSuccessEvent> {

	private final MogulService ms;

	MogulAuthenticationSuccessEventListener(MogulService ms) {
		this.ms = ms;
	}

	@Override
	public void onApplicationEvent(AuthenticationSuccessEvent ase) {
		this.ms.login(ase.getAuthentication());
	}

}
