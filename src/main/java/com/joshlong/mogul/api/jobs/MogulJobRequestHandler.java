package com.joshlong.mogul.api.jobs;

import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.Map;

public class MogulJobRequestHandler implements JobRequestHandler<MogulJobRequest> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<String, Job> jobs;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactionTemplate;

	public MogulJobRequestHandler(Map<String, Job> jobs, ApplicationEventPublisher publisher,
			TransactionTemplate transactionTemplate) {
		this.jobs = jobs;
		this.publisher = publisher;
		this.transactionTemplate = transactionTemplate;
		Assert.notNull(this.jobs, "the jobs must not be null");
		Assert.notNull(this.publisher, "the publisher must not be null");
		Assert.notNull(this.transactionTemplate, "the transactionTemplate must not be null");
	}

	private void publishInTransaction(Object event) {
		this.transactionTemplate.executeWithoutResult(_ -> this.publisher.publishEvent(event));
	}

	@Override
	public void run(MogulJobRequest request) {
		var jobName = request.jobName();
		var job = this.jobs.get(jobName);
		Assert.notNull(job, () -> "there is no job named [" + jobName + "] to run!");
		var context = new MapJobExecutionContext(request.mogulId(), request.context());
		this.publishInTransaction(new JobStartedEvent(jobName, request.mogulId()));
		var result = (JobExecutionResult) null;
		try {
			result = job.run(context);
		} //
		catch (Throwable throwable) {
			this.log.error("the job named [{}] for mogul [{}] failed", jobName, request.mogulId(), throwable);
			result = JobExecutionResult.error(throwable);
		}
		this.publishInTransaction(new JobStoppedEvent(jobName, request.mogulId(), result.success()));
		if (!result.success()) {
			var cause = result.context().get(Job.EXCEPTION_KEY);
			throw new IllegalStateException("the job named [" + jobName + "] failed",
					cause instanceof Throwable throwable ? throwable : null);
		}
	}

}
