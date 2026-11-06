package com.joshlong.mogul.api.publications;

import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * publish a pending publication. this lands in jobrunr's tables as json, so it carries
 * ids and nothing secret: the context, settings and tokens stay encrypted in the
 * {@code publication} row, and are read back from there when the job runs.
 *
 * @param gatesPassed true once every {@link PublicationGate} has let the publication
 * through, so that the job a gate resumes doesn't ask them all over again
 */
public record PublishRequest(Long publicationId, Long mogulId, boolean gatesPassed) implements JobRequest {

	@Override
	public Class<? extends JobRequestHandler<?>> getJobRequestHandler() {
		return PublishRequestHandler.class;
	}

}
