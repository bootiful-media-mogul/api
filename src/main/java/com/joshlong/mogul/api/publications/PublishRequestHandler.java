package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.PublisherPlugin;
import org.jobrunr.jobs.lambdas.JobRequestHandler;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

/**
 * where every publication actually happens, whether it was asked for right now or for
 * next week. the publication is read back from its row, the plugin is run as the mogul
 * who asked for it, and the outcome is recorded -- which is what tells the client, by way
 * of the notifications, that it's done.
 * <p>
 * jobrunr retries a job that throws, and a replay must not publish twice: anything that
 * isn't {@link Publication.State#PENDING pending} any more has already been dealt with,
 * and is left alone.
 */
public class PublishRequestHandler implements JobRequestHandler<PublishRequest> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * all of these are looked up when they're first needed: jobrunr's storage needs every
	 * handler before the scheduler can exist, and the publication service, the plugins
	 * and the gates reach, one way or another, most of the rest of the application.
	 */
	private final ObjectProvider<DefaultPublicationService> publications;

	private final ObjectProvider<PublisherPlugin<?>> plugins;

	private final ObjectProvider<PublicationGate> gates;

	private final ObjectProvider<JobRequestScheduler> jobs;

	PublishRequestHandler(ObjectProvider<DefaultPublicationService> publications,
			ObjectProvider<PublisherPlugin<?>> plugins, ObjectProvider<PublicationGate> gates,
			ObjectProvider<JobRequestScheduler> jobs) {
		this.publications = publications;
		this.plugins = plugins;
		this.gates = gates;
		this.jobs = jobs;
	}

	@Override
	public void run(PublishRequest request) {
		var service = this.publications.getObject();
		var publication = service.getPublicationById(request.publicationId());
		if (publication == null) {
			this.log.warn("there is no publication {} to publish", request.publicationId());
			return;
		}
		if (publication.state() != Publication.State.PENDING) {
			this.log.debug("publication {} is {}, not pending; nothing to do", publication.id(), publication.state());
			return;
		}
		var plugin = this.plugins.stream().filter(p -> p.name().equals(publication.plugin())).findFirst().orElse(null);
		if (plugin == null) {
			service.fail(publication, publication.plugin(),
					"there is no plugin named [" + publication.plugin() + "] any more");
			return;
		}
		service.runAs(publication, () -> this.publish(service, publication, plugin, request.gatesPassed()));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void publish(DefaultPublicationService service, Publication publication, PublisherPlugin plugin,
			boolean gatesPassed) {
		var attempt = (PublicationService.PublicationAttempt<Publishable>) null;
		try {
			attempt = service.attemptFor(publication, plugin);
		} //
		catch (RuntimeException e) {
			service.fail(publication, plugin.name(), e.getMessage());
			return;
		}
		if (!gatesPassed) {
			// a gate resumes on whichever thread its wait ends on -- a listener, usually,
			// with no mogul in sight -- so the rest of the work goes back to jobrunr
			var resume = (Runnable) () -> this.jobs.getObject()
				.enqueue(new PublishRequest(publication.id(), publication.mogulId(), true));
			for (var gate : this.gates.orderedStream().toList()) {
				if (gate.defer(attempt, resume)) {
					this.log.debug("publication {} was deferred by {}", publication.id(), gate.getClass().getName());
					return;
				}
			}
		}
		service.completePublication(attempt);
	}

}
