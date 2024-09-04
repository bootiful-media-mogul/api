package com.joshlong.mogul.api.managedfiles;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

class ManagedFileDeletionRequestRowMapper implements RowMapper<ManagedFileDeletionRequest> {

	@Override
	public ManagedFileDeletionRequest mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new ManagedFileDeletionRequest(rs.getLong("id"), rs.getLong("mogul"), rs.getString("bucket"),
				rs.getString("folder"), rs.getString("filename"), rs.getString("storage_filename"),
				rs.getBoolean("deleted"), rs.getDate("created"));
	}

}
