package com.joshlong.mogul.api.publications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

class PublicationRowMapper implements RowMapper<Publication> {

	private final ObjectMapper objectMapper;

	private final TextEncryptor textEncryptor;

	private final Logger log = LoggerFactory.getLogger(getClass());

	PublicationRowMapper(ObjectMapper objectMapper, TextEncryptor textEncryptor) {
		this.objectMapper = objectMapper;
		this.textEncryptor = textEncryptor;
	}

	private Class<?> classFor(String name) {
		try {
			Assert.hasText(name, "you must provide a non-empty class name");
			return Class.forName(name);
		}
		catch (ClassNotFoundException e) {
			log.warn("classNotFoundException when trying to do Class.forName({}) ", name, e);
		}
		return null;
	}

	@Override
	public Publication mapRow(ResultSet rs, int rowNum) throws SQLException {
		var context = readContextFor(rs.getString("context"));
		var payload = this.textEncryptor.decrypt(rs.getString("payload"));
		return new Publication(rs.getLong("mogul_id"), rs.getLong("id"), rs.getString("plugin"), rs.getDate("created"),
				rs.getDate("published"), context, payload, classFor(rs.getString("payload_class")));
	}

	private Map<String, String> readContextFor(String context) {
		var decrypted = this.textEncryptor.decrypt(context);
		// @formatter:off
		var parameterizedTypeReference = new TypeReference<Map<String, String>>() {};
		// @formatter:on
		try {
			return this.objectMapper.readValue(decrypted, parameterizedTypeReference);
		} //
		catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

}
