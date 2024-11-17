package com.joshlong.mogul.api.compositions;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Function;

/**
 * have to be careful to select all the attachments
 */

class CompositionRowMapper implements RowMapper<Composition> {

	/**
	 * were going to load all the attachments at the same time and then map them with
	 * this.
	 */
	private final Function<Long, Collection<Attachment>> attachmentsResolver;

	CompositionRowMapper(Function<Long, Collection<Attachment>> attachmentsResolver) {
		this.attachmentsResolver = attachmentsResolver;
	}

	@Override
	public Composition mapRow(ResultSet rs, int rowNum) throws SQLException {
		try {
			var id = rs.getLong("id");
			return new Composition(id, rs.getString("payload"), Class.forName(rs.getString("payload_class")),
					rs.getString("field"), this.attachmentsResolver.apply(id));
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
