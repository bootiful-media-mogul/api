package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

class PublicationRowMapper implements RowMapper<Publication> {

	// @formatter:off
	private final ParameterizedTypeReference<Map<String, String>> mapParameterizedTypeReference = new ParameterizedTypeReference<>() {};
	// @formatter:on

	private final TextEncryptor textEncryptor;

	private final Logger log = LoggerFactory.getLogger(getClass());

	PublicationRowMapper(TextEncryptor textEncryptor) {
		this.textEncryptor = textEncryptor;
	}

	private Class<?> classFor(String name) {
		try {
			Assert.hasText(name, "you must provide a non-empty class name");
			return Class.forName(name);
		}
		catch (ClassNotFoundException e) {
			log.warn(String.format(
					"classNotFoundException when trying to do Class.forName(%s) to resolve the class for a %s ", name,
					Publication.class.getName()), e);
		}
		return null;
	}

	@Override
	public Publication mapRow(ResultSet rs, int rowNum) throws SQLException {
		var context = this.readContext(rs.getString("context"));
		var payload = rs.getString("payload");
		var url = rs.getString("url");
		var state = rs.getString("state");
		var stateEnum = Publication.State.valueOf(state);
		Assert.notNull(state, "state must not be null");
		Assert.notNull(stateEnum, "stateEnum must not be null");
		return new Publication( //
				rs.getLong("mogul"), //
				rs.getLong("id"), //
				rs.getString("plugin"), //
				rs.getDate("created"), //
				rs.getDate("published"), //
				context, //
				payload, //
				classFor(rs.getString("payload_class")), //
				url, stateEnum);

	}

	private Map<String, String> readContext(String context) {
		var decrypted = this.textEncryptor.decrypt(context);
		return JsonUtils.read(decrypted, this.mapParameterizedTypeReference);
	}

}
