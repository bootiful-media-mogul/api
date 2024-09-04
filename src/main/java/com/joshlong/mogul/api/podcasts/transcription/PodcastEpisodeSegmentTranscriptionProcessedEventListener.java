package com.joshlong.mogul.api.podcasts.transcription;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.PodcastEpisodeCompletionEvent;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.Segment;
import com.joshlong.mogul.api.transcription.TranscriptionClient;
import com.joshlong.mogul.api.transcription.TranscriptionProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
class PodcastEpisodeSegmentTranscriptionProcessedEventListener {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final PodcastService podcastService;

	private final TranscriptionClient transcriptionClient;

	private final ManagedFileService managedFileService;

	PodcastEpisodeSegmentTranscriptionProcessedEventListener(PodcastService podcastService,
			TranscriptionClient transcriptionClient, ManagedFileService managedFileService) {
		this.podcastService = podcastService;
		this.transcriptionClient = transcriptionClient;
		this.managedFileService = managedFileService;
	}

	@ApplicationModuleListener
	void onPodcastEpisodeCompletionEvent(PodcastEpisodeCompletionEvent pce) {
		if (pce.episode().complete()) {
			for (var seg : pce.episode().segments()) {
				// transcription is expensive so let's not do it unnecessarily
				if (seg.transcribable() && !StringUtils.hasText(seg.transcript())) {
					this.transcriptionClient
						.startTranscription(new PodcastEpisodeSegmentTranscribable(seg, this.managedFileService));
				}
			}
		}
	}

	// todo: important!
	// do <EM>not</em> serialize the {@link Resource resource} pointed to in
	// this event by making this a Spring Modulith {@link ApplicationModuleListener}! is
	// there some way to tell Spring Modulith to exclude
	// certain fields from the persistence?
	@EventListener
	void onPodcastEpisodeSegmentTranscription(TranscriptionProcessedEvent processedEvent) throws Exception {
		var key = processedEvent.key();
		var txt = processedEvent.transcript();
		var clzz = processedEvent.subject();
		if (StringUtils.hasText(txt) && clzz.getName().equals(Segment.class.getName())
				&& key instanceof Number number) {
			var id = number.longValue();
			this.log.debug(
					"the id of the podcast episode segment that we're updating is" + " {} and the transcript is {}", id,
					txt);
			this.podcastService.setPodcastEpisodesSegmentTranscript(id, true, txt);
		}

	}

}
