package com.joshlong.mogul.api.mogul;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
@Transactional
class DefaultMogulService implements MogulService {

	//
	private final static String PODBEAN_ACCOUNTS_SETTINGS = "podbean";

	private final static String PODBEAN_ACCOUNTS_SETTINGS_CLIENT_ID = "client-id";

	private final static String PODBEAN_ACCOUNTS_SETTINGS_CLIENT_SECRET = "client-secret";

	private final JdbcClient db;

	private final TransactionTemplate transactionTemplate;

	private final ApplicationEventPublisher publisher;

	private final MogulRowMapper mogulRowMapper = new MogulRowMapper();

	DefaultMogulService(JdbcClient jdbcClient, TransactionTemplate transactionTemplate, 	ApplicationEventPublisher publisher) {
		this.db = jdbcClient;
		this.transactionTemplate = transactionTemplate;
		this.publisher = publisher;
		Assert.notNull(this.db, "the db is null");
		Assert.notNull(this.transactionTemplate, "the transactionTemplate is null");
	}

	@Override
	public Mogul getCurrentMogul() {
		var name = SecurityContextHolder.getContextHolderStrategy().getContext().getAuthentication().getName();
		return this.getMogulByName(name);
	}

	@Override
	public Mogul login(Authentication principal) {
		var principalName = principal.getName();
		var exists = getMogulByName(principalName) != null;
		var sql = """
				insert into mogul(username,  client_id) values (?, ?)
				on conflict on constraint mogul_client_id_username_key do nothing
				""";
		if (principal.getPrincipal() instanceof Jwt jwt && jwt.getClaims().get("aud") instanceof List list
				&& list.get(0) instanceof String aud) {
			this.db.sql(sql).params(principalName, aud).update();
		}
		var mogul = this.getMogulByName(principalName);
		if (!exists) {
			publisher.publishEvent(new MogulCreatedEvent(mogul));
		}
		return mogul;
	}

	@Override
	public Mogul getMogulById(Long id) {
		return this.db.sql("select * from mogul where id =? ").param(id).query(this.mogulRowMapper).single();
	}

	@Override
	public Mogul getMogulByName(String name) {
		var moguls = this.db//
			.sql("select * from mogul where  username  = ? ")
			.param(name)
			.query(this.mogulRowMapper)
			.list();
		Assert.state(moguls.size() <= 1, "there should only be one mogul with this username [" + name + "]");
		return moguls.isEmpty() ? null : moguls.getFirst();
	}

	@Override
	public void assertAuthorizedMogul(Long aLong) {
		var currentlyAuthenticated = getCurrentMogul();
		Assert.state(currentlyAuthenticated != null && currentlyAuthenticated.id().equals(aLong),
				"the requested mogul [" + aLong + "] is not currently authenticated");
	}

}

class MogulRowMapper implements RowMapper<Mogul> {

	@Override
	public Mogul mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new Mogul(rs.getLong("id"), rs.getString("username"), rs.getString("email"), rs.getString("client_id"));
	}

}