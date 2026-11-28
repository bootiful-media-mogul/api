package com.joshlong.mogul.api.analysis;

import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import java.util.Locale;
import java.util.Map;

/**
 * analyzes what's asked for, in the background, and tells the studio when it's done with
 * an {@code analysis-completed-event}, keyed by {@code type:id}. a replayed request just
 * analyzes again, and the new analysis replaces the old one.
 */
@Component
@RegisterReflectionForBinding(AnalysisRequestedEvent.class)
class Analyses {

	/** how an analysis turned out, as the studio is told */
	enum Outcome {

		/** there's a new analysis */
		ANALYZED,

		/** there was no text to analyze, by the time it came to it */
		EMPTY,

		/** something went wrong reading the text or asking the model */
		FAILED

	}

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final AnalysisService analysisService;

	private final ContentAnalyzer analyzer;

	private final MogulService mogulService;

	Analyses(AnalysisService analysisService, ContentAnalyzer analyzer, MogulService mogulService) {
		this.analysisService = analysisService;
		this.analyzer = analyzer;
		this.mogulService = mogulService;
	}

	/**
	 * not in a transaction: the model can take a minute over an hour of talk, and nothing
	 * here needs a connection held open while it thinks. the write at the end is its own
	 * transaction.
	 */
	@ApplicationModuleListener(propagation = Propagation.NOT_SUPPORTED)
	void onAnalysisRequested(AnalysisRequestedEvent event) {
		try {
			var sections = this.analysisService.sections(event.mogulId(), event.id(), event.type());
			if (sections.isEmpty()) {
				this.notify(event, Outcome.EMPTY);
				return;
			}
			var language = this.mogulService.getMogulById(event.mogulId()).language();
			var highlights = this.analyzer.analyze(sections, language);
			this.analysisService.write(event.mogulId(), event.id(), event.type(), highlights);
			this.notify(event, Outcome.ANALYZED);
		} //
		catch (RuntimeException e) {
			this.log.warn("couldn't analyze {} #{}", event.type(), event.id(), e);
			this.notify(event, Outcome.FAILED);
		}
	}

	private void notify(AnalysisRequestedEvent event, Outcome outcome) {
		var context = Map.of("type", event.type(), "id", event.id(), "outcome",
				outcome.name().toLowerCase(Locale.ROOT));
		NotificationEvents.notifyAsync(NotificationEvent.systemNotificationEventFor(event.mogulId(),
				new AnalysisCompletedEvent(event.mogulId(), event.type(), event.id(), outcome),
				event.type() + ":" + event.id(), JsonUtils.write(context)));
	}

	/** what the studio is told, under the category {@code analysis-completed-event} */
	record AnalysisCompletedEvent(Long mogulId, String type, Long id, Outcome outcome) {
	}

}
