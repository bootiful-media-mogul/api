package com.joshlong.mogul.api.mogul;

import java.time.LocalDate;
import java.util.Collection;

public interface MogulStatusService {

	MogulStatus today(Long mogulId);

	/**
	 * the status for {@code date}, created if the mogul has none yet: publishing about a
	 * day ahead of time needs the day to exist before anything has happened on it.
	 */
	MogulStatus forDate(Long mogulId, LocalDate date);

	MogulStatus getMogulStatusById(Long id);

	Collection<MogulStatus> getMogulStatusesByIds(Collection<Long> ids);

	MogulStatus getMogulStatusByDate(Long mogulId, LocalDate date);

	Collection<MogulStatus> getRecentMogulStatuses(Long mogulId, int limit);

}
