package com.joshlong.mogul.api.ayrshare;

import com.joshlong.mogul.api.compositions.ComposableOwnerResolver;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

@Component
class AyrshareComposableOwnerResolver implements ComposableOwnerResolver {

	private final JdbcClient db;

	AyrshareComposableOwnerResolver(JdbcClient db) {
		this.db = db;
	}

	@Override
	public boolean supports(Class<?> composableClass) {
		return AyrsharePublicationComposition.class.equals(composableClass);
	}

	@Override
	public Long mogulId(Class<?> composableClass, Long compositionKey) {
		return this.db.sql("select mogul_id from ayrshare_publication_composition where id = ?")
			.param(compositionKey)
			.query(Long.class)
			.optional()
			.orElse(null);
	}

}
