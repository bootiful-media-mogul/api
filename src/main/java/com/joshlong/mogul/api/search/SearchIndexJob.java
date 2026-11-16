package com.joshlong.mogul.api.search;

import com.joshlong.mogul.api.jobs.Job;
import com.joshlong.mogul.api.jobs.JobExecutionContext;
import com.joshlong.mogul.api.jobs.JobExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * rebuilds the mogul's part of the search index from the database: their posts,
 * transcribed segments, and notes, each filed under them. it's on the studio's jobs page;
 * run it after a deploy that changes what's indexed, or whenever search looks wrong.
 */
@Component
class SearchIndexJob implements Job {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final SearchService searchService;

	SearchIndexJob(SearchService searchService) {
		this.searchService = searchService;
	}

	@Override
	public JobExecutionResult run(JobExecutionContext context) {
		var written = this.searchService.rebuild(context.mogulId());
		this.log.info("the search index for mogul {} has {} entries", context.mogulId(), written);
		return JobExecutionResult.ok();
	}

}
