package com.joshlong.mogul.api.mogul;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * a new mogul has to have a given name, but auth0's {@code /userinfo} doesn't always have
 * one: a GitHub login comes with a {@code name} and a {@code nickname} instead. creating
 * the mogul for one failed on the {@code not null} constraint on {@code given_name}.
 */
class UserInfoNamesTest {

	private static DefaultMogulService.UserInfo userinfo(String givenName, String familyName, String name,
			String nickname) {
		return new DefaultMogulService.UserInfo("github|54473", null, givenName, familyName, name, nickname, null,
				null);
	}

	@Test
	void givenAndFamilyNamesWinWhenThereAreSome() {
		var userinfo = userinfo("Josh", "Long", "Joshua Long", "starbuxman");
		assertThat(userinfo.givenNameOrElse("github|54473")).isEqualTo("Josh");
		assertThat(userinfo.familyNameOrNull()).isEqualTo("Long");
	}

	@Test
	void aGithubLoginSplitsItsName() {
		var userinfo = userinfo(null, null, " Josh  Long ", "starbuxman");
		assertThat(userinfo.givenNameOrElse("github|54473")).isEqualTo("Josh");
		assertThat(userinfo.familyNameOrNull()).isEqualTo("Long");
	}

	@Test
	void aOneWordNameIsAllGivenName() {
		var userinfo = userinfo(null, null, "Josh", "starbuxman");
		assertThat(userinfo.givenNameOrElse("github|54473")).isEqualTo("Josh");
		assertThat(userinfo.familyNameOrNull()).isNull();
	}

	@Test
	void withNoNameTheNicknameWillDo() {
		var userinfo = userinfo(null, null, "", "starbuxman");
		assertThat(userinfo.givenNameOrElse("github|54473")).isEqualTo("starbuxman");
		assertThat(userinfo.familyNameOrNull()).isNull();
	}

	@Test
	void withNothingAtAllTheUsernameWillDo() {
		var userinfo = userinfo(null, null, null, null);
		assertThat(userinfo.givenNameOrElse("github|54473")).isEqualTo("github|54473");
		assertThat(userinfo.familyNameOrNull()).isNull();
	}

}
