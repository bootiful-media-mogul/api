package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

class SegmentRowMapper implements RowMapper<Segment> {

	private final Function<Long, ManagedFile> managedFileFunction;

	SegmentRowMapper(Function<Long, ManagedFile> managedFileFunction) {
		this.managedFileFunction = managedFileFunction;
	}

	@Override
	public Segment mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new Segment(rs.getLong("podcast_episode_id"), rs.getLong("id"),
				managedFileFunction.apply(rs.getLong("segment_audio_managed_file_id")),
				managedFileFunction.apply(rs.getLong("produced_segment_audio_managed_file_id")),
				rs.getLong("cross_fade_duration"), rs.getString("name"), rs.getInt("sequence_number"),
				rs.getBoolean("transcribable"), rs.getString("transcript"));
	}

}