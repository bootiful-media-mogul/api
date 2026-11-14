package com.joshlong.mogul.api.mogul;

import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.stereotype.Controller;

import java.time.LocalDate;
import java.util.Collection;

@Controller
class MogulStatusController {

	private static final int DEFAULT_RECENT_LIMIT = 10;

	private final MogulService mogulService;

	private final MogulStatusService mogulStatusService;

	MogulStatusController(MogulService mogulService, MogulStatusService mogulStatusService) {
		this.mogulService = mogulService;
		this.mogulStatusService = mogulStatusService;
	}

	@QueryMapping
	MogulStatus mogulStatusToday() {
		return this.mogulStatusService.today(this.mogulService.getCurrentMogul().id());
	}

	@QueryMapping
	MogulStatus mogulStatusForDate(@Argument String date) {
		return this.mogulStatusService.forDate(this.mogulService.getCurrentMogul().id(), LocalDate.parse(date));
	}

	@QueryMapping
	Collection<MogulStatus> mogulStatuses(@Argument Integer limit) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		return this.mogulStatusService.getRecentMogulStatuses(mogulId,
				null == limit || limit <= 0 ? DEFAULT_RECENT_LIMIT : limit);
	}

	@SchemaMapping
	String date(MogulStatus mogulStatus) {
		return mogulStatus.date().toString();
	}

}
