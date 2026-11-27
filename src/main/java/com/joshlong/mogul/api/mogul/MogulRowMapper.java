package com.joshlong.mogul.api.mogul;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

class MogulRowMapper implements RowMapper<Mogul> {

	@Override
	public Mogul mapRow(ResultSet rs, int rowNum) throws SQLException {
		// getTimestamp, not getDate: `updated` is a timestamptz, and getDate would hand
		// back a java.sql.Date with the time truncated to midnight. nothing reads
		// Mogul.updated() today, which is the only reason that has been invisible.
		return new Mogul(rs.getLong("id"), rs.getString("username"), rs.getString("email"), rs.getString("client_id"),
				rs.getString("given_name"), rs.getString("family_name"), rs.getTimestamp("updated"),
				rs.getString("time_zone"), rs.getString("language"));
	}

}
