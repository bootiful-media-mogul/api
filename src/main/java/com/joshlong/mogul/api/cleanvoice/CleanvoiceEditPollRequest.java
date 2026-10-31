package com.joshlong.mogul.api.cleanvoice;

import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

import java.util.Map;

/**
 * check on a cleanvoice edit. this lands in jobrunr's tables as json, so it carries ids
 * and nothing secret: the key is looked up again, by mogul, every time it runs.
 */
public record CleanvoiceEditPollRequest(Long mogulId, String editId, Map<String, Object> context,
		int attempt) implements JobRequest {

	@Override
	public Class<? extends JobRequestHandler<?>> getJobRequestHandler() {
		return CleanvoiceEditPollHandler.class;
	}

}
