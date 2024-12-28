package com.joshlong.mogul.api.mogul;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Transactional
@ImportRuntimeHints(DefaultMogulService.Hints.class)
class DefaultMogulService implements MogulService {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			var values = MemberCategory.values();
			for (var c : Set.of(UserInfo.class)) {
				hints.reflection().registerType(c, values);
			}
		}

	}

	private final RestClient userinfoHttpRestClient = RestClient.builder().build();

	private final int minutesSinceLastUserinfoEndpointRequest = 5;

	private final Map<String, Mogul> mogulsByName = new ConcurrentHashMap<>();

	private final Map<Long, Mogul> mogulsById = new ConcurrentHashMap<>();

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final JdbcClient db;

	private final ApplicationEventPublisher publisher;

	private final MogulRowMapper mogulRowMapper = new MogulRowMapper();

	private final TransactionTemplate transactionTemplate;

	private final String auth0Userinfo;

	DefaultMogulService(@Value("${auth0.userinfo}") String auth0Userinfo, JdbcClient jdbcClient,
			ApplicationEventPublisher publisher, TransactionTemplate transactionTemplate) {

		this.auth0Userinfo = auth0Userinfo;
		this.transactionTemplate = transactionTemplate;

		this.db = jdbcClient;
		this.publisher = publisher;
		Assert.notNull(this.db, "the db is null");
		try (var scheduledExecutorService = Executors.newScheduledThreadPool(1);) {
			scheduledExecutorService.scheduleAtFixedRate(() -> {
				mogulsById.clear();
				mogulsByName.clear();
				log.debug("mogul cache eviction...");
			}, 1, 5, TimeUnit.MINUTES);
		}
	}

	@Override
	public Mogul getCurrentMogul() {
		var name = SecurityContextHolder.getContextHolderStrategy().getContext().getAuthentication().getName();
		return this.getMogulByName(name);
	}

	// just for the first time login
	private record UserInfo(String sub, @JsonProperty("username") String username,
			@JsonProperty("given_name") String givenName, @JsonProperty("family_name") String familyName,
			String nickname, String picture, String email) {

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

			mogulByName = this.getRecentlyLoggedInMogulByUsername(username);
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

	private Mogul getRecentlyLoggedInMogulByUsername(String username) {
		var sql = " select * from mogul where username = ? and updated  > ( NOW() - INTERVAL '"
				+ this.minutesSinceLastUserinfoEndpointRequest + " minute ' ) ";
		var mogul = this.db.sql(sql).param(username).query(this.mogulRowMapper).list();
		if (!mogul.isEmpty()) {
			var validMatchingMogul = mogul.getFirst();
			this.db//
				.sql(" update mogul set updated = NOW() where username = ?  and id = ?  ")
				.param(username) //
				.param(validMatchingMogul.id())//
				.update();//
			this.log.trace(
					"found a recent mogul by name [{}] in the database, so no need to hit the /userinfo endpoint.",
					username);
			return validMatchingMogul;
		}
		return null;
	}

	/**
	 * adapts calls to {@link this#login(String, String, String, String, String)}
	 */
	private Mogul loginByPrincipal(JwtAuthenticationToken principal) {
		var username = principal.getName();
		this.log.trace("logging in mogul [{}] with client id [{}]", username, principal.getName());

		var mogul = this.getRecentlyLoggedInMogulByUsername(username);
		if (null == mogul) {
			if (principal.getPrincipal() instanceof Jwt jwt && jwt.getClaims().get("aud") instanceof List<?> list
					&& list.getFirst() instanceof String aud) {
				this.log.trace(
						"could NOT find a recent mogul by name [{}] in the database, so we'll have to hit the /userinfo endpoint.",
						username);
				var accessToken = principal.getToken().getTokenValue();
				// var uri = this.auth0Domain + "/userinfo";
				var userinfo = this.userinfoHttpRestClient.get()
					.uri(this.auth0Userinfo)
					.headers(httpHeaders -> httpHeaders.setBearerAuth(accessToken))
					.retrieve()
					.body(UserInfo.class);
				mogul = this.login(userinfo.sub(), aud, userinfo.email(), userinfo.givenName(), userinfo.familyName());

			}
		}
		this.nonNullMogul(mogul, username);
		return mogul;
	}

	@Override
	public Mogul getMogulById(Long id) {
		var msg = new StringBuilder();
		msg.append("trying to resolve mogul by id ").append(id);
		var res = this.mogulsById.computeIfAbsent(id, mogulId -> {
			var mogul = this.db.sql("select * from mogul where id =? ").param(id).query(this.mogulRowMapper).single();
			msg.append(", cache missed, resolving by db query [").append(mogulId).append("]");
			return mogul;
		});
		if (this.log.isTraceEnabled())
			this.log.trace(msg.toString());
		return res;
	}

	@Override
	public Mogul getMogulByName(String name) {
		var msg = new StringBuilder();
		msg.append("trying to resolve mogul by name [").append(name).append("]");
		var res = this.mogulsByName.computeIfAbsent(name, key -> {
			var moguls = this.db//
				.sql("select * from mogul where username = ? ")
				.param(key)
				.query(this.mogulRowMapper)
				.list();
			Assert.state(moguls.size() <= 1, "there should only be one mogul with this username [" + name + "]");
			var mogul = moguls.isEmpty() ? null : moguls.getFirst();
			msg.append(", but had to hit the DB to find a mogul by name [").append(name).append("]");
			return mogul;
		});
		if (log.isTraceEnabled())
			log.trace(msg.toString());
		return res;
	}

	@Override
	public void assertAuthorizedMogul(Long mogulId) {
		var currentlyAuthenticated = this.getCurrentMogul();
		Assert.state(currentlyAuthenticated != null && currentlyAuthenticated.id().equals(mogulId),
				"the requested mogul [" + mogulId + "] is not currently authenticated");
	}

	private static class MogulRowMapper implements RowMapper<Mogul> {

		@Override
		public Mogul mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Mogul(rs.getLong("id"), rs.getString("username"), rs.getString("email"),
					rs.getString("client_id"), rs.getString("given_name"), rs.getString("family_name"),
					rs.getDate("updated"));
		}

	}

	@EventListener
	void authenticationSuccessEvent(AuthenticationSuccessEvent ase) {
		this.log.trace("handling authentication success event for {}", ase.getAuthentication().getName());
		this.transactionTemplate.execute(status -> {
			var authentication = (JwtAuthenticationToken) ase.getAuthentication();
			this.loginByPrincipal(authentication);
			return null;
		});
	}

}
