package com.joshlong.mogul.api.mogul;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;

@Transactional
class DefaultMogulStatusService implements MogulStatusService {

	private final JdbcClient db;

	private final MogulService mogulService;

	private final MogulStatusRowMapper mogulStatusRowMapper = new MogulStatusRowMapper();

	DefaultMogulStatusService(JdbcClient db, MogulService mogulService) {
		this.db = db;
		this.mogulService = mogulService;
		Assert.notNull(this.db, "the db is null");
	}

	@Override
	public MogulStatus today(Long mogulId) {
		return this.forDate(mogulId, LocalDate.now(this.zoneFor(mogulId)));
	}

	@Override
	public MogulStatus forDate(Long mogulId, LocalDate date) {
		this.db//
			.sql("insert into mogul_status(mogul_id, date) values (?,?) on conflict (mogul_id, date) do nothing")//
			.params(mogulId, date)//
			.update();
		var status = this.getMogulStatusByDate(mogulId, date);
		Assert.notNull(status, "the status for mogul [" + mogulId + "] on [" + date + "] should exist by now");
		return status;
	}

	/**
	 * the one place that decides what day it is. it has to be the mogul's day, not the
	 * server's: the JVM runs in UTC, so a mogul in Los Angeles publishing at 6pm was
	 * filed under tomorrow, and one in Tokyo would spend most of their waking day filed
	 * under yesterday. falls back to the server's zone for a mogul who hasn't told us
	 * where they are.
	 */
	private ZoneId zoneFor(Long mogulId) {
		var mogul = this.mogulService.getMogulById(mogulId);
		var timeZone = (null == mogul) ? null : mogul.timeZone();
		return StringUtils.hasText(timeZone) ? ZoneId.of(timeZone) : ZoneId.systemDefault();
	}

	@Override
	public MogulStatus getMogulStatusById(Long id) {
		var all = this.db //
			.sql("select * from mogul_status where id = ?") //
			.param(id) //
			.query(this.mogulStatusRowMapper) //
			.list();
		return all.isEmpty() ? null : all.getFirst();
	}

	@Override
	public Collection<MogulStatus> getMogulStatusesByIds(Collection<Long> ids) {
		if (ids.isEmpty())
			return List.of();
		return this.db //
			.sql("select * from mogul_status where id = any(?)") //
			.params(new SqlArrayValue("bigint", ids.toArray())) //
			.query(this.mogulStatusRowMapper) //
			.list();
	}

	@Override
	public MogulStatus getMogulStatusByDate(Long mogulId, LocalDate date) {
		var all = this.db //
			.sql("select * from mogul_status where mogul_id = ? and date = ?") //
			.params(mogulId, date) //
			.query(this.mogulStatusRowMapper) //
			.list();
		return all.isEmpty() ? null : all.getFirst();
	}

	@Override
	public Collection<MogulStatus> getRecentMogulStatuses(Long mogulId, int limit) {
		if (limit <= 0)
			return List.of();
		// a status row is created the moment a mogul opens the app, so most days exist
		// without anything having been published on them. filtering in the query rather
		// than after it means the limit counts days that have something to show -- ask
		// for ten and you get the last ten days with publications, not ten rows of which
		// nine are empty.
		return this.db //
			.sql("""
					select ms.* from mogul_status ms
					where ms.mogul_id = ?
					  and exists (select 1 from publication p
					              where p.payload = ms.id::text
					                and p.payload_class = ?)
					order by ms.date desc
					limit ?
					""") //
			.params(mogulId, MogulStatus.class.getName(), limit) //
			.query(this.mogulStatusRowMapper) //
			.list();
	}

	private static class MogulStatusRowMapper implements RowMapper<MogulStatus> {

		@Override
		public MogulStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new MogulStatus(rs.getLong("id"), rs.getLong("mogul_id"), rs.getObject("date", LocalDate.class),
					rs.getTimestamp("created"));
		}

	}

}
