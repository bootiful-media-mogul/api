package com.joshlong.mogul.api.mogul;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
class DefaultMogulService implements MogulService {

	private final Map<String, Mogul> mogulsByName = new ConcurrentHashMap<>();
	private final Map<Long, Mogul> mogulsById = new ConcurrentHashMap<>();

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final TransactionTemplate tt;
	private final JdbcClient db;

	private final ApplicationEventPublisher publisher;

	private final MogulRowMapper mogulRowMapper = new MogulRowMapper();

	DefaultMogulService(JdbcClient jdbcClient,
						TransactionTemplate tt,
						ApplicationEventPublisher publisher) {
		this.db = jdbcClient;
		this.publisher = publisher;
		this.tt = tt;
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

	@Override
	public Mogul login(Authentication principal) {
		var principalName = principal.getName();
		var mogulByName = (Mogul) null;
		if ((mogulByName = this.getMogulByName(principalName)) == null) {
			if (principal.getPrincipal() instanceof Jwt jwt && jwt.getClaims().get("aud") instanceof List list
					&& list.getFirst() instanceof String aud) {
				var sql = """
						insert into mogul(username,  client_id) values (?, ?)
						on conflict on constraint mogul_client_id_username_key do nothing
						""";
				this.db.sql(sql).params(principalName, aud).update();
			}
			mogulByName = this.getMogulByName(principalName);
			publisher.publishEvent(new MogulCreatedEvent(mogulByName));
		}
		return mogulByName;
	}

	@Override
	public Mogul getMogulById(Long id) {
		var msg = new StringBuilder();
		msg.append("trying to resolve mogul by id " + id + "");
		var res = this.mogulsById.computeIfAbsent(id, mogulId -> {
			var mogul = this.db.sql("select * from mogul where id =? ").param(id).query(this.mogulRowMapper).single();
			msg.append(", cache missed, resolving by db query [").append(mogulId).append("]");
			return mogul;
		});
		if (log.isDebugEnabled()) {
			log.debug(msg.toString());
		}
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
			msg.append(", but had to hit the DB to find a mogul by name [")
					.append(name)
					.append("]");
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

	static class MogulRowMapper implements RowMapper<Mogul> {

		@Override
		public Mogul mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Mogul(rs.getLong("id"), rs.getString("username"), rs.getString("email"),
					rs.getString("client_id"));
		}

	}

}
