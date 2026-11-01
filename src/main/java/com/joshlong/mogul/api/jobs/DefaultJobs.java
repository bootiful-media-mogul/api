package com.joshlong.mogul.api.jobs;

import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

class DefaultJobs implements Jobs {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<String, Job> jobs;

	private final JobRequestScheduler jobScheduler;

	DefaultJobs(Map<String, Job> jobs, JobRequestScheduler jobScheduler) {
		this.jobs = jobs;
		this.jobScheduler = jobScheduler;
		Assert.notNull(this.jobs, "the jobs must not be null");
		Assert.notNull(this.jobScheduler, "the jobScheduler must not be null");
	}

	@Override
	public Map<String, Job> jobs() {
		return Map.copyOf(this.jobs);
	}

	@Override
	public void launch(Long mogulId, String jobName, Map<String, Object> context) throws JobException {
		Assert.notNull(mogulId, "the mogulId must not be null");
		if (!this.jobs.containsKey(jobName))
			throw new JobException("there is no job named [" + jobName + "]");
		var attributes = new HashMap<String, Object>(context == null ? Map.of() : context);
		// the mogul is the one attribute every job is promised, and the one the caller
		// has no business overriding.
		attributes.put(Job.MOGUL_ID_KEY, mogulId);
		var id = this.jobScheduler.enqueue(new MogulJobRequest(jobName, mogulId, attributes));
		this.log.debug("enqueued the job named [{}] for mogul [{}] as [{}]", jobName, mogulId, id);
	}

}
