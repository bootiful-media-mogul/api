package com.joshlong.mogul.api.mogul;

import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

@Controller
class MogulController {

	private final MogulService mogulService;

	MogulController(MogulService mogulService) {
		this.mogulService = mogulService;
	}

	@QueryMapping
	Map<String, Object> me(Principal principal) {
		var mogulByName = mogulService.getMogulByName(principal.getName());
		var map = new HashMap<String, Object>();
		map.put("name", mogulByName.username());
		map.put("email", mogulByName.email());
		map.put("givenName", mogulByName.givenName());
		map.put("id", mogulByName.id());
		map.put("familyName", mogulByName.familyName());
		// null until the mogul tells us; the client uses that to decide whether to offer
		// up the zone its browser reports.
		map.put("timeZone", mogulByName.timeZone());
		// null until the mogul picks one; the client adopts its own when this is empty.
		map.put("language", mogulByName.language());
		return map;
	}

	@MutationMapping
	boolean setMogulTimeZone(@Argument String timeZone) {
		this.mogulService.setTimeZone(this.mogulService.getCurrentMogul().id(), timeZone);
		return true;
	}

	@MutationMapping
	boolean setMogulLanguage(@Argument String language) {
		this.mogulService.setLanguage(this.mogulService.getCurrentMogul().id(), language);
		return true;
	}

}
