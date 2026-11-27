package com.joshlong.mogul.api.mogul;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.cache.Cache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Transactional
@ImportRuntimeHints(DefaultMogulService.Hints.class)
class DefaultMogulService implements MogulService {

	private final RestClient userinfoHttpRestClient = RestClient.builder().build();

	private final Cache mogulsById;

	private final Cache mogulsByName;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final JdbcClient db;

	private final ApplicationEventPublisher publisher;

	private final MogulRowMapper mogulRowMapper = new MogulRowMapper();

	private final TransactionTemplate transactionTemplate;

	private final String auth0Userinfo;

	DefaultMogulService(String auth0Userinfo, JdbcClient jdbcClient, ApplicationEventPublisher publisher,
			TransactionTemplate transactionTemplate, Cache mogulsById, Cache mogulsByName) {
		this.auth0Userinfo = auth0Userinfo;
		this.transactionTemplate = transactionTemplate;
		this.db = jdbcClient;
		this.publisher = publisher;
		// these were node-local maps. a mogul edited on one node stayed stale on every
		// other until the entry aged out -- and since today() reads the mogul's time
		// zone, that showed up as dates rendering differently depending on which pod
		// answered. they are caches from the broadcasting manager now, so an eviction
		// here is an eviction everywhere, the way Settings has always done it.
		this.mogulsById = mogulsById;
		this.mogulsByName = mogulsByName;
		Assert.notNull(this.db, "the db is null");
		Assert.notNull(this.mogulsById, "the moguls-by-id cache is null");
		Assert.notNull(this.mogulsByName, "the moguls-by-name cache is null");
	}

	@Override
	public Mogul getCurrentMogul() {
		var name = SecurityContextHolder.getContextHolderStrategy().getContext().getAuthentication().getName();
		return this.getMogulByName(name);
	}

	@Override
	public Mogul login(String username, String clientId, String email, String first, String last) {
		this.log.debug("logging in mogul [{}] with client id [{}] and email [{}]", username, clientId, email);
		var mogulByName = (Mogul) null;
		if ((mogulByName = this.getMogulByName(username)) == null) {
			var sql = """
					insert into mogul(username,  client_id , email, given_name, family_name,updated) values (?, ?,?, ?,?,NOW())
					on conflict on constraint mogul_client_id_username_key do  update set updated = NOW()
					""";
			this.db.sql(sql)
				.params(username, //
						clientId, //
						email, //
						first, //
						last //
				)//
				.update();

			mogulByName = this.getMogulByName(username);
			this.nonNullMogul(mogulByName, username);
			this.publisher.publishEvent(new MogulCreatedEvent(mogulByName));
		} //
		this.nonNullMogul(mogulByName, username);
		this.publisher.publishEvent(new MogulAuthenticatedEvent(mogulByName));
		return mogulByName;
	}

	private void nonNullMogul(Mogul mogul, String username) {
		Assert.notNull(mogul, "the mogul by name [" + username + "] is null");
	}

	/**
	 * adapts calls to {@link this#login(String, String, String, String, String)}
	 */
	private Mogul doLoginByPrincipal(JwtAuthenticationToken principal) {
		var username = principal.getName();
		this.log.trace("logging in mogul [{}] with client id [{}]", username, principal.getName());
		var mogul = this.getMogulByName(username);
		if (null == mogul) {
			if (principal.getPrincipal() instanceof Jwt jwt && jwt.getClaims().get("aud") instanceof List<?> list
					&& list.getFirst() instanceof String aud) {
				this.log.trace(
						"could NOT find a recent mogul by name [{}] in the database, so we'll have to hit the /userinfo endpoint.",
						username);
				var accessToken = principal.getToken().getTokenValue();
				var userinfo = this.userinfoHttpRestClient //
					.get()//
					.uri(this.auth0Userinfo)//
					.headers(httpHeaders -> httpHeaders.setBearerAuth(accessToken))//
					.retrieve()//
					.body(UserInfo.class);
				Assert.notNull(userinfo, "auth0 had no userinfo for [" + username + "]");
				mogul = this.login(userinfo.sub(), aud, userinfo.email(), userinfo.givenNameOrElse(username),
						userinfo.familyNameOrNull());
			}
		}
		this.nonNullMogul(mogul, username);
		return mogul;
	}

	@Override
	public Mogul getMogulById(Long id) {
		var hit = this.mogulsById.get(id, Mogul.class);
		if (hit != null) {
			this.logMogulCacheAttempt(id, "id", false);
			return hit;
		}
		var mogul = this.db //
			.sql("select * from mogul where id =? ") //
			.param(id) //
			.query(this.mogulRowMapper) //
			.single();
		this.mogulsById.put(id, mogul);
		this.logMogulCacheAttempt(id, "id", true);
		return mogul;
	}

	@Override
	public Mogul getMogulByName(String name) {
		var hit = this.mogulsByName.get(name, Mogul.class);
		if (hit != null) {
			this.logMogulCacheAttempt(name, "name", false);
			return hit;
		}
		var moguls = this.db//
			.sql("select * from mogul where username = ? ")
			.param(name)
			.query(this.mogulRowMapper)
			.list();
		Assert.state(moguls.size() <= 1, "there should only be one mogul with this username [" + name + "]");
		var mogul = moguls.isEmpty() ? null : moguls.getFirst();
		// an absent mogul is deliberately not cached. the old ConcurrentMap never
		// stored a null from computeIfAbsent, but this cache would happily keep one,
		// and login() inserts the row then reads it straight back through here -- a
		// cached miss would make that read fail every time.
		if (mogul != null)
			this.mogulsByName.put(name, mogul);
		this.logMogulCacheAttempt(name, "name", true);
		return mogul;
	}

	@Override
	public Collection<Mogul> getMogulByEmail(String email) {
		return db.sql("select * from mogul where email = ? ").params(email).query(this.mogulRowMapper).list();
	}

	private void logMogulCacheAttempt(Object input, String type, boolean resolved) {
		this.log.trace("tried to resolve the mogul by {} with input [{}] and found it {}.", type, input,
				resolved ? "in the DB" : "in the cache");
	}

	@Override
	public Mogul setTimeZone(Long mogulId, String timeZone) {
		this.assertAuthorizedMogul(mogulId);
		// rejects anything that isn't a real zone before it reaches the database, so a
		// junk value can't make today() throw on every subsequent request.
		var zone = ZoneId.of(timeZone);
		this.db.sql("update mogul set time_zone = ? where id = ?").params(zone.getId(), mogulId).update();
		this.evictMogulFromCaches(mogulId);
		return this.getMogulById(mogulId);
	}

	@Override
	public Mogul setLanguage(Long mogulId, String language) {
		this.assertAuthorizedMogul(mogulId);
		// a well-formed BCP 47 tag, stored in its normalised form. anything else would be
		// spliced into every prompt for this mogul, so it is refused here instead.
		var locale = Locale.forLanguageTag(language == null ? "" : language.trim());
		Assert.hasText(locale.getLanguage(), "the language [" + language + "] is not a language tag");
		this.db.sql("update mogul set language = ? where id = ?").params(locale.toLanguageTag(), mogulId).update();
		this.evictMogulFromCaches(mogulId);
		return this.getMogulById(mogulId);
	}

	private void evictMogulFromCaches(Long mogulId) {
		// the by-name entry is keyed on the username, so resolve it from the database
		// rather than from this node's cache: another node may hold that entry while
		// this one never cached it at all, and then nothing would ever evict it.
		var username = this.db //
			.sql("select username from mogul where id = ?") //
			.param(mogulId) //
			.query(String.class) //
			.optional() //
			.orElse(null);
		this.mogulsById.evict(mogulId);
		if (username != null)
			this.mogulsByName.evict(username);
	}

	@Override
	public void assertAuthorizedMogul(Long mogulId) {
		var currentlyAuthenticated = this.getCurrentMogul();
		Assert.state(currentlyAuthenticated != null && currentlyAuthenticated.id().equals(mogulId),
				"the requested mogul [" + mogulId + "] is not currently authenticated");
	}

	@EventListener
	void authenticationSuccessEvent(AuthenticationSuccessEvent ase) {
		this.transactionTemplate.execute(_ -> {
			var authentication = (JwtAuthenticationToken) ase.getAuthentication();
			this.doLoginByPrincipal(authentication);
			return null;
		});
	}

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			var values = MemberCategory.values();
			for (var c : Set.of(UserInfo.class)) {
				hints.reflection().registerType(c, values);
			}
		}

	}

	// just for the first time login
	/**
	 * what auth0's {@code /userinfo} says about a person. not every connection fills in
	 * the same claims: a GitHub login has a {@code name} and a {@code nickname} but no
	 * {@code given_name} or {@code family_name}, and often no {@code email}.
	 */
	record UserInfo(String sub, @JsonProperty("username") String username, @JsonProperty("given_name") String givenName,
			@JsonProperty("family_name") String familyName, String name, String nickname, String picture,
			String email) {

		/**
		 * a mogul has to have a given name: the {@code given_name}, or else the first
		 * word of the {@code name}, or else the {@code nickname}, or else the fallback.
		 */
		String givenNameOrElse(String fallback) {
			if (StringUtils.hasText(this.givenName))
				return this.givenName.strip();
			if (StringUtils.hasText(this.name))
				return this.name.strip().split("\\s+", 2)[0];
			if (StringUtils.hasText(this.nickname))
				return this.nickname.strip();
			return fallback;
		}

		/**
		 * the {@code family_name}, or else whatever of the {@code name} comes after the
		 * given name, when the given name came from it.
		 */
		String familyNameOrNull() {
			if (StringUtils.hasText(this.familyName))
				return this.familyName.strip();
			if (!StringUtils.hasText(this.givenName) && StringUtils.hasText(this.name)) {
				var parts = this.name.strip().split("\\s+", 2);
				return parts.length == 2 ? parts[1] : null;
			}
			return null;
		}

	}

}
