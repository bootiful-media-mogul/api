package com.joshlong.mogul.api.mogul;

import com.joshlong.mogul.api.Notable;

import java.util.Date;

/**
 * the main tenant/user of this system.
 * <p>
 * deliberately <em>not</em> {@link com.joshlong.mogul.api.Publishable}: a mogul is an
 * identity, not an artifact, and it never ends, so publications attached to one would
 * accumulate without bound. publish a {@link MogulStatus} instead.
 */
public record Mogul(Long id, String username, String email, String clientId, String givenName, String familyName,
		Date updated, String timeZone, String language) implements Notable {

	@Override
	public Long notableKey() {
		return this.id;
	}
}
