package com.joshlong.mogul.api.mogul;

import org.springframework.context.event.EventListener;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.stereotype.Component;

/**
 * invoked immediately after the first OAuth dance redirects to the client which then
 * proxies to this resource server
 */
@Component
class MogulAuthenticationSuccessEventListener {

	private final MogulService ms;

	MogulAuthenticationSuccessEventListener(MogulService ms) {
		this.ms = ms;
	}

	@EventListener
	void authenticationSuccessEvent(AuthenticationSuccessEvent ase) {
		this.ms.login(ase.getAuthentication());
	}

}
