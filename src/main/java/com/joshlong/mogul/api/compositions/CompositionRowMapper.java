package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.Publication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

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

	private final Logger log = LoggerFactory.getLogger(getClass());

	CompositionRowMapper(Function<Long, Collection<Attachment>> attachmentsResolver) {
		this.attachmentsResolver = attachmentsResolver;
	}

	@Override
	public Composition mapRow(ResultSet rs, int rowNum) throws SQLException {
		var id = rs.getLong("id");
		return new Composition(id, rs.getString("payload"), classFor(rs.getString("payload_class")),
				rs.getString("field"), this.attachmentsResolver.apply(id));
	}

	private Class<?> classFor(String name) {
		try {
			Assert.hasText(name, "you must provide a non-empty class name");
			return Class.forName(name);
		}
		catch (ClassNotFoundException e) {
			log.warn("classNotFoundException when trying to do Class.forName({}) to resolve the class for a {} ", name,
					Publication.class.getName(), e);
		}
		return null;
	}

}
