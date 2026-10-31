package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.cleanvoice.Cleanvoice;
import com.joshlong.cleanvoice.model.Edit;
import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * looks at a cleanvoice edit once, and either announces how it ended or schedules itself
 * to look again. a fresh job each time, rather than one that sleeps for ten minutes, so
 * it ties up no worker while it waits and survives this node going away.
 */
public class CleanvoiceEditPollHandler implements JobRequestHandler<CleanvoiceEditPollRequest> {

	/** three hours of polling, which is many times what cleanvoice ever needs */
	static final int MAX_ATTEMPTS = 360;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Cleanvoice cleanvoice;

	/**
	 * looked up when it's first needed: jobrunr's storage needs every handler before the
	 * scheduler can exist, so a handler that needs the scheduler up front is a cycle.
	 */
	private final ObjectProvider<JobRequestScheduler> jobs;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactions;

	CleanvoiceEditPollHandler(Cleanvoice cleanvoice, ObjectProvider<JobRequestScheduler> jobs,
			ApplicationEventPublisher publisher, TransactionTemplate transactions) {
		this.cleanvoice = cleanvoice;
		this.jobs = jobs;
		this.publisher = publisher;
		this.transactions = transactions;
	}

	@Override
	public void run(CleanvoiceEditPollRequest request) {
		var mogulId = request.mogulId();
		var edit = (Edit) null;
		try {
			edit = CleanvoiceTenant.callAs(mogulId, () -> this.cleanvoice.getEdit(request.editId()));
		} //
		catch (Exception e) {
			var diagnosis = CleanvoiceErrors.diagnose(e);
			// a flaky poll is worth another try; a revoked key or empty account isn't
			if (!diagnosis.actionRequired() && request.attempt() + 1 < MAX_ATTEMPTS) {
				this.log.warn("couldn't check on cleanvoice edit {}; trying again: {}", request.editId(),
						diagnosis.reason());
				this.again(request);
				return;
			}
			this.fail(request, diagnosis.reason(), diagnosis.actionRequired());
			return;
		}
		if (edit.isSuccess()) {
			var result = edit.result();
			var labels = labelsUrl(result.timestampsMarkersUrls());
			if (result.downloadUrl() == null || labels == null) {
				this.fail(request, "cleanvoice finished but didn't say where the results are", false);
				return;
			}
			this.log.debug("cleanvoice edit {} for mogul #{} is done", request.editId(), mogulId);
			this.publish(new AudioCleanedEvent(mogulId, result.downloadUrl(), labels, request.context()));
			return;
		}
		if (edit.isFailure()) {
			this.fail(request, "cleanvoice couldn't clean the audio", false);
			return;
		}
		if (request.attempt() + 1 >= MAX_ATTEMPTS) {
			this.fail(request, "cleanvoice was still working after three hours", false);
			return;
		}
		this.again(request);
	}

	private void again(CleanvoiceEditPollRequest request) {
		this.jobs.getObject()
			.schedule(Instant.now().plus(DefaultAudioCleaning.POLL_INTERVAL), new CleanvoiceEditPollRequest(
					request.mogulId(), request.editId(), request.context(), request.attempt() + 1));
	}

	private void fail(CleanvoiceEditPollRequest request, String reason, boolean actionRequired) {
		this.log.warn("cleanvoice edit {} for mogul #{} failed: {}", request.editId(), request.mogulId(), reason);
		this.publish(new AudioCleaningFailedEvent(request.mogulId(), reason, actionRequired, request.context()));
	}

	private void publish(Object event) {
		this.transactions.executeWithoutResult(_ -> this.publisher.publishEvent(event));
	}

	/**
	 * cleanvoice hands back the same edits in half a dozen formats. the audacity timeline
	 * is the one with every muted region in it, one per line, in seconds.
	 */
	static String labelsUrl(Object markers) {
		var urls = new ArrayList<String>();
		collect(markers, urls);
		return urls.stream()
			.filter(url -> url.replaceAll("[?].*", "").contains("timeline_audacity"))
			.findFirst()
			.orElse(null);
	}

	private static void collect(Object node, List<String> urls) {
		if (node instanceof String s && s.startsWith("http"))
			urls.add(s);
		else if (node instanceof Map<?, ?> map)
			map.values().forEach(value -> collect(value, urls));
		else if (node instanceof List<?> list)
			list.forEach(value -> collect(value, urls));
	}

}
