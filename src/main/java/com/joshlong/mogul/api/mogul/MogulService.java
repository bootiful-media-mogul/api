package com.joshlong.mogul.api.mogul;

import java.util.Collection;

public interface MogulService {

	Mogul getCurrentMogul();

	Mogul login(String username, String clientId, String email, String first, String last);

	Mogul getMogulById(Long id);

	Mogul getMogulByName(String name);

	Collection<Mogul> getMogulByEmail(String email);

	void assertAuthorizedMogul(Long aLong);

	/**
	 * records where the mogul is, as an IANA zone id ({@code Asia/Tokyo}). this decides
	 * which calendar day their work is filed under, so it is a stored preference rather
	 * than something read off whichever device they happen to be using.
	 */
	Mogul setTimeZone(Long mogulId, String timeZone);

	/**
	 * records the language the mogul's system is written in, as a BCP 47 tag ({@code es},
	 * {@code fr-CA}). every prompt sent to the model for this mogul asks for its answer
	 * in this language, so it is stored on the account rather than read off the device.
	 */
	Mogul setLanguage(Long mogulId, String language);

}
