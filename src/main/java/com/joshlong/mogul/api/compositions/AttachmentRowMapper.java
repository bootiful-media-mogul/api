package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

class AttachmentRowMapper implements RowMapper<Attachment> {

	private final Function<Long, ManagedFile> managedFileFunction;

	AttachmentRowMapper(Function<Long, ManagedFile> managedFileFunction) {
		this.managedFileFunction = managedFileFunction;
	}

	@Override
	public Attachment mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new Attachment(rs.getLong("id"), rs.getString("key"),
				this.managedFileFunction.apply(rs.getLong("managed_file_id")));
	}

}
