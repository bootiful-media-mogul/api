package com.joshlong.mogul.api.mogul;

public interface MogulService {

	Mogul getCurrentMogul();

	Mogul login(String username, String clientId, String email, String first, String last);

	Mogul getMogulById(Long id);

	Mogul getMogulByName(String name);

	void assertAuthorizedMogul(Long aLong);

}
