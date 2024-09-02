package com.joshlong.mogul.api.mogul;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@RegisterReflectionForBinding(DefaultMogulService.MogulJwtAuthenticationTokenDetails.class)
class DefaultMogulService implements MogulService {

	private final String auth0Domain;

	private final Map<String, Mogul> mogulsByName = new ConcurrentHashMap<>();

	private final Map<Long, Mogul> mogulsById = new ConcurrentHashMap<>();

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final JdbcClient db;

	private final ApplicationEventPublisher publisher;

	private final MogulRowMapper mogulRowMapper = new MogulRowMapper();

	DefaultMogulService(@Value("${auth0.domain}") String auth0Domain, JdbcClient jdbcClient,
				ApplicationEventPublisher publisher) {
		this.auth0Domain = auth0Domain;
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
	private record UserInfo(String sub, @JsonProperty("given_name") String givenName,
							@JsonProperty("family_name") String familyName, String nickname, String picture, String email) {
	}

	@Override
	public Mogul login(Authentication principal) {
		var principalName = principal.getName();
		var mogulByName = (Mogul) null;
		if ((mogulByName = this.getMogulByName(principalName)) == null) {
			if (principal.getPrincipal() instanceof Jwt jwt && jwt.getClaims().get("aud") instanceof List list
					&& list.getFirst() instanceof String aud) {
				var accessToken = ((JwtAuthenticationToken) principal).getToken().getTokenValue();
				var uri = this.auth0Domain + "/userinfo";
				var rc = RestClient.builder().build();
				var userinfo = rc.get()
					.uri(uri)
					.headers(httpHeaders -> httpHeaders.setBearerAuth(accessToken))
					.retrieve()
					.body(UserInfo.class);
				if (log.isDebugEnabled())
					log.debug("userinfo: {}", userinfo.email());

				var sql = """
						insert into mogul(username,  client_id , email, given_name, family_name) values (?, ?,?, ?,?)
						on conflict on constraint mogul_client_id_username_key do  nothing
						""";
				this.db.sql(sql)
					.params(principalName, //
							aud, //
							userinfo.email(), //
							userinfo.givenName(), //
							userinfo.familyName() //
					)//
					.update();
			} //
			else {
				log.debug("not a valid authentication!");
			}
			mogulByName = this.getMogulByName(principalName);
			this.publisher.publishEvent(new MogulCreatedEvent(mogulByName));
		}
		this.publisher.publishEvent(new MogulAuthenticatedEvent(mogulByName));
		return mogulByName;
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
		this.log.debug(msg.toString());
		return res;
	}

	@Override
	public Mogul getMogulByName(String name) {
		var msg = new StringBuilder();
		msg.append("trying to resolve mogul by name [").append(name).append("]");
		var res = this.mogulsByName.computeIfAbsent(name, key -> {
			var moguls = this.db//
				.sql("select * from mogul where  username = ? ")
				.param(key)
				.query(this.mogulRowMapper)
				.list();
			Assert.state(moguls.size() <= 1, "there should only be one mogul with this username [" + name + "]");
			var mogul = moguls.isEmpty() ? null : moguls.getFirst();
			msg.append(", but had to hit the DB to find a mogul by name [").append(name).append("]");
			return mogul;
		});
		if (log.isDebugEnabled())
			log.debug(msg.toString());
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
					rs.getString("client_id"), rs.getString("given_name"), rs.getString("family_name"));
		}

	}

	///

	@EventListener
	void authenticationSuccessEvent(AuthenticationSuccessEvent ase) {

		var authentication = (JwtAuthenticationToken) ase.getAuthentication();

		this.login(authentication);
	}

	static class MogulJwtAuthenticationToken extends JwtAuthenticationToken {

		private final MogulJwtAuthenticationTokenDetails details;

		MogulJwtAuthenticationTokenDetails details() {
			return details;
		}

		public MogulJwtAuthenticationToken(JwtAuthenticationToken delegate,
				MogulJwtAuthenticationTokenDetails details) {
			super(delegate.getToken(), delegate.getAuthorities(), delegate.getName());
			this.details = details;
		}

	}

	record MogulJwtAuthenticationTokenDetails(@JsonProperty("family_name") String familyName,
			@JsonProperty("given_name") String givenName, String picture, String email) {
	}

}
