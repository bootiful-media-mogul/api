package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.cleanvoice.Cleanvoice;
import com.joshlong.cleanvoice.model.EditConfig;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.storage.Storage;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

class DefaultAudioCleaning implements AudioCleaning {

	/**
	 * how long cleanvoice has to fetch the original. it fetches it when the edit starts,
	 * but a queue on their side shouldn't be what makes this fail.
	 */
	private static final Duration FETCH_WINDOW = Duration.ofHours(6);

	/**
	 * cleanvoice takes about ten minutes for forty-five of audio; no point asking sooner
	 */
	static final Duration FIRST_POLL = Duration.ofMinutes(1);

	static final Duration POLL_INTERVAL = Duration.ofSeconds(30);

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Cleanvoice cleanvoice;

	private final Storage storage;

	/**
	 * looked up when it's first needed: the scheduler needs jobrunr's storage, which
	 * needs every job handler, and some of those need services that need this.
	 */
	private final ObjectProvider<JobRequestScheduler> jobs;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactions;

	DefaultAudioCleaning(Cleanvoice cleanvoice, Storage storage, ObjectProvider<JobRequestScheduler> jobs,
			ApplicationEventPublisher publisher, TransactionTemplate transactions) {
		this.cleanvoice = cleanvoice;
		this.storage = storage;
		this.jobs = jobs;
		this.publisher = publisher;
		this.transactions = transactions;
	}

	/**
	 * the settings that keep cleaned audio aligned with the original: {@code muted}
	 * silences what it would otherwise cut. {@code keepMusic} stops it from treating an
	 * intro bed as noise and deleting it. flac, so the render isn't a second generation
	 * of mp3.
	 */
	static EditConfig config() {
		return EditConfig.builder()
			.removeNoise(EditConfig.NoiseRemoval.ON)
			.normalize(true)
			.keepMusic(true)
			.fillers(true)
			.stutters(true)
			.longSilences(true)
			.muted(true)
			.exportTimestamps(true)
			.exportFormat(EditConfig.ExportFormat.FLAC)
			.build();
	}

	@Override
	public void clean(Long mogulId, ManagedFile input, Map<String, Object> context) {
		Assert.state(input.written(), () -> "ManagedFile #" + input.id() + " has nothing in it to clean");
		try {
			var url = this.storage.temporaryReadUri(input.bucket(), input.key(), FETCH_WINDOW);
			var created = CleanvoiceTenant.callAs(mogulId, () -> this.cleanvoice.createEdit(url.toString(), config()));
			this.log.debug("started cleanvoice edit {} for ManagedFile #{} of mogul #{}", created.id(), input.id(),
					mogulId);
			this.jobs.getObject()
				.schedule(Instant.now().plus(FIRST_POLL),
						new CleanvoiceEditPollRequest(mogulId, created.id(), context, 0));
		} //
		catch (Exception e) {
			var diagnosis = CleanvoiceErrors.diagnose(e);
			this.log.warn("couldn't start cleaning ManagedFile #{} for mogul #{}: {}", input.id(), mogulId,
					diagnosis.reason(), e);
			this.transactions.executeWithoutResult(_ -> this.publisher.publishEvent(
					new AudioCleaningFailedEvent(mogulId, diagnosis.reason(), diagnosis.actionRequired(), context)));
		}
	}

}
