package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Transcript;
import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.media.AudioWaveformComputedEvent;
import com.joshlong.mogul.api.media.MediaNormalizationFailedEvent;
import com.joshlong.mogul.api.media.MediaNormalizedEvent;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.api.transcripts.TranscriptService;
import com.joshlong.mogul.utils.DateUtils;
import com.joshlong.mogul.utils.JsonUtils;
import graphql.schema.DataFetchingEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.graphql.data.method.annotation.*;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Controller
@RegisterReflectionForBinding({ Map.class, KeptRange.class })
class PodcastController {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final MogulService mogulService;

	private final PodcastService podcastService;

	private final TranscriptService transcriptService;

	PodcastController(MogulService mogulService, PodcastService podcastService, TranscriptService transcriptService) {
		this.mogulService = mogulService;
		this.podcastService = podcastService;
		this.transcriptService = transcriptService;
	}

	@QueryMapping
	Collection<Episode> podcastEpisodesByPodcast(@Argument Long podcastId,
			DataFetchingEnvironment dataFetchingEnvironment) {
		var graphic = isFieldRequested(dataFetchingEnvironment, "graphic");
		var segments = isFieldRequested(dataFetchingEnvironment, "segments");
		return this.podcastService.getPodcastEpisodesByPodcast(podcastId, graphic && segments);
	}

	private boolean isFieldRequested(DataFetchingEnvironment env, String fieldName) {
		return env.getSelectionSet().contains(fieldName);
	}

	@MutationMapping
	boolean movePodcastEpisodeSegmentDown(@Argument Long podcastEpisodeId, @Argument Long podcastEpisodeSegmentId) {
		this.podcastService.movePodcastEpisodeSegmentDown(podcastEpisodeId, podcastEpisodeSegmentId);
		return true;
	}

	@MutationMapping
	boolean movePodcastEpisodeSegmentUp(@Argument Long podcastEpisodeId, @Argument Long podcastEpisodeSegmentId) {
		this.podcastService.movePodcastEpisodeSegmentUp(podcastEpisodeId, podcastEpisodeSegmentId);
		return true;
	}

	@SchemaMapping
	Collection<Episode> episodes(Podcast podcast) {
		return podcastService.getPodcastEpisodesByPodcast(podcast.id(), false);
	}

	@MutationMapping
	boolean updatePodcastEpisode(@Argument Long podcastEpisodeId, @Argument String title, @Argument String description,
			@Argument OffsetDateTime created) {
		this.podcastService.updatePodcastEpisodeDetails(podcastEpisodeId, title, description,
				DateUtils.toDate(created));
		return true;
	}

	@QueryMapping
	Episode podcastEpisodeById(@Argument Long podcastEpisodeId) {
		return this.podcastService.getPodcastEpisodeById(podcastEpisodeId);
	}

	@QueryMapping
	Segment podcastEpisodeSegmentById(@Argument Long podcastEpisodeSegmentId) {
		return this.podcastService.getPodcastEpisodeSegmentById(this.mogulService.getCurrentMogul().id(),
				podcastEpisodeSegmentId);
	}

	@MutationMapping
	boolean editPodcastEpisodeSegment(@Argument Long podcastEpisodeSegmentId, @Argument List<KeptRange> kept) {
		this.podcastService.editPodcastEpisodeSegment(this.mogulService.getCurrentMogul().id(), podcastEpisodeSegmentId,
				kept);
		return true;
	}

	@MutationMapping
	boolean requestPodcastEpisodeSegmentWaveform(@Argument Long podcastEpisodeSegmentId) {
		this.podcastService.requestPodcastEpisodeSegmentWaveform(this.mogulService.getCurrentMogul().id(),
				podcastEpisodeSegmentId);
		return true;
	}

	@MutationMapping
	Long createPodcastEpisodeSegment(@Argument Long podcastEpisodeId) {
		var mogul = this.mogulService.getCurrentMogul().id();
		var segment = this.podcastService.createPodcastEpisodeSegment(mogul, podcastEpisodeId, "", 0);
		return segment.id();
	}

	@SchemaMapping
	OffsetDateTime created(Podcast podcast) {
		return DateUtils.forDate(podcast.created());
	}

	@SchemaMapping
	OffsetDateTime created(Episode episode) {
		return DateUtils.forDate(episode.created());
	}

	@QueryMapping
	Podcast podcastById(@Argument Long podcastId) {
		return this.podcastService.getPodcastById(podcastId);
	}

	@QueryMapping
	Collection<Podcast> podcasts() {
		var currentMogul = this.mogulService.getCurrentMogul();
		return this.podcastService.getAllPodcastsByMogul(currentMogul.id());
	}

	@MutationMapping
	boolean deletePodcastEpisode(@Argument Long podcastEpisodeId) {
		this.podcastService.deletePodcastEpisode(podcastEpisodeId);
		return true;
	}

	@MutationMapping
	boolean deletePodcastEpisodeSegment(@Argument Long podcastEpisodeSegmentId) {
		this.podcastService.deletePodcastEpisodeSegment(podcastEpisodeSegmentId);
		return true;
	}

	@MutationMapping
	Episode createPodcastEpisodeDraft(@Argument Long podcastId, @Argument String title, @Argument String description) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		return this.podcastService.createPodcastEpisodeDraft(mogulId, podcastId, title, description);
	}

	@MutationMapping
	boolean deletePodcast(@Argument Long podcastId) {
		var podcast = this.podcastService.getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast is null");
		var mogulId = podcast.mogulId();
		this.mogulService.assertAuthorizedMogul(mogulId);
		var podcasts = this.podcastService.getAllPodcastsByMogul(mogulId);
		Assert.state(!podcasts.isEmpty() && podcasts.size() - 1 > 0,
				"you must have at least one active, non-deleted podcast");
		this.podcastService.deletePodcast(podcast.id());
		return true;
	}

	@BatchMapping
	Map<Episode, List<Segment>> segments(List<Episode> episodes) {
		var epIds = episodes.stream().map(Episode::id).collect(Collectors.toSet());
		var allEpisodeSegments = this.podcastService.getPodcastEpisodeSegmentsByEpisodes(epIds);
		// key off the episode instances we were handed. re-reading the episodes and
		// keying off *those* meant that any drift between the stored copy and the
		// parent instance -- Episode is a record, so equality is every field -- built a
		// map the batch loader couldn't match, and every segments field silently
		// resolved to null. it was also a query for something we already had.
		var map = new LinkedHashMap<Episode, List<Segment>>();
		for (var episode : episodes)
			map.put(episode, allEpisodeSegments.get(episode.id()));

		for (var ep : map.entrySet()) {
			if (ep.getValue() == null) {
				// an episode is meant to always have at least one segment. throwing here
				// failed the entire query, not just this field, so a single episode left
				// segment-less took its whole podcast page down. report it and render
				// the episode empty instead.
				this.log.warn("no segments found for episode id #{}, and there should be at least one",
						ep.getKey().id());
				ep.setValue(new ArrayList<>());
				continue;
			}
			ep.getValue().sort(Comparator.comparingInt(Segment::order));
		}
		return map;
	}

	@SchemaMapping
	Composition titleComposition(Episode episode) {
		return this.podcastService.getPodcastEpisodeTitleComposition(episode.id());
	}

	@SchemaMapping
	Composition descriptionComposition(Episode episode) {
		return this.podcastService.getPodcastEpisodeDescriptionComposition(episode.id());
	}

	@MutationMapping
	boolean updatePodcast(@Argument Long podcastId, @Argument String title) {
		this.podcastService.updatePodcast(podcastId, title);
		return true;
	}

	@MutationMapping
	Podcast createPodcast(@Argument String title) {
		return this.podcastService.createPodcast(this.mogulService.getCurrentMogul().id(), title);
	}

	@ApplicationModuleListener
	void podcastCreatedEventNotifyingListener(PodcastCreatedEvent event) {
		this.doNotify(event, event.podcast());
	}

	@ApplicationModuleListener
	void podcastDeletedEventNotifyingListener(PodcastDeletedEvent event) {
		this.doNotify(event, event.podcast());
	}

	@ApplicationModuleListener
	void podcastUpdatedEventNotifyingListener(PodcastUpdatedEvent event) {
		this.doNotify(event, event.podcast());
	}

	private void doNotify(Object event, Podcast podcast) {
		var ne = NotificationEvent.visibleNotificationEventFor(podcast.mogulId(), event, Long.toString(podcast.id()),
				podcast.title());
		NotificationEvents.notify(ne);
	}

	@BatchMapping
	Map<Episode, Float> duration(List<Episode> episodes) {
		var episodeIds = episodes.stream().map(Episode::id).collect(Collectors.toSet());
		var durations = this.podcastService.getPodcastEpisodeDurationsByEpisodes(episodeIds);
		var map = new LinkedHashMap<Episode, Float>();
		for (var episode : episodes)
			map.put(episode, durations.getOrDefault(episode.id(), 0L).floatValue());
		return map;
	}

	@ApplicationModuleListener
	void broadcastPodcastEpisodeCompletionEventToClients(PodcastEpisodeCompletedEvent podcastEpisodeCompletedEvent) {
		var episode = podcastEpisodeCompletedEvent.episode();
		var id = episode.id();
		try {
			var map = Map.of("episodeId", id, "complete", episode.complete());
			var json = JsonUtils.write(map);
			var notificationEvent = NotificationEvent.systemNotificationEventFor(podcastEpisodeCompletedEvent.mogulId(),
					podcastEpisodeCompletedEvent, Long.toString(episode.id()), json);
			NotificationEvents.notify(notificationEvent);
		} //
		catch (Exception e) {
			this.log.warn("experienced an exception when trying to emit "
					+ "a podcast completed event for podcast episode id # {}", id);
		} //
	}

	@ApplicationModuleListener
	void onMediaNormalizedEvent(MediaNormalizedEvent mediaNormalizedEvent) {
		var context = mediaNormalizedEvent.context();
		if (!context.containsKey(DefaultPodcastService.PODCAST_EPISODE_CONTEXT_KEY)
				|| !context.containsKey(DefaultPodcastService.PODCAST_EPISODE_SEGMENT_CONTEXT_KEY))
			return;
		var episodeId = context.get(DefaultPodcastService.PODCAST_EPISODE_CONTEXT_KEY);
		var segmentId = context.get(DefaultPodcastService.PODCAST_EPISODE_SEGMENT_CONTEXT_KEY);
		var duration = context.getOrDefault(MediaNormalizedEvent.DURATION_IN_MILLISECONDS, 0L);
		try {
			var json = JsonUtils.write(Map.of(//
					"episodeId", episodeId, //
					"segmentId", segmentId, //
					MediaNormalizedEvent.DURATION_IN_MILLISECONDS, duration));
			var notificationEvent = NotificationEvent.systemNotificationEventFor(mediaNormalizedEvent.in().mogulId(),
					mediaNormalizedEvent, Long.toString((Long) segmentId), json);
			NotificationEvents.notify(notificationEvent);
		} //
		catch (Exception e) {
			this.log.warn("couldn't emit a media normalized notification for podcast episode segment # {}", segmentId,
					e);
		}
	}

	@ApplicationModuleListener
	void onAudioWaveformComputedEvent(AudioWaveformComputedEvent event) {
		var outcome = new HashMap<String, Object>();
		outcome.put("success", event.success());
		if (!event.success())
			outcome.put("error", event.error());
		this.notifySegmentClients(event.in().mogulId(), event, event.context(), outcome);
	}

	@ApplicationModuleListener
	void onMediaNormalizationFailedEvent(MediaNormalizationFailedEvent event) {
		this.notifySegmentClients(event.in().mogulId(), event, event.context(), Map.of("error", event.error()));
	}

	/**
	 * tells the clients about something that happened to one segment, keyed by its id,
	 * with the episode and segment ids in the context like the media normalized
	 * notification.
	 */
	private void notifySegmentClients(Long mogulId, Object event, Map<String, Object> context,
			Map<String, Object> extra) {
		if (!context.containsKey(DefaultPodcastService.PODCAST_EPISODE_CONTEXT_KEY)
				|| !context.containsKey(DefaultPodcastService.PODCAST_EPISODE_SEGMENT_CONTEXT_KEY))
			return;
		var segmentId = context.get(DefaultPodcastService.PODCAST_EPISODE_SEGMENT_CONTEXT_KEY);
		try {
			var payload = new HashMap<String, Object>(extra);
			payload.put("episodeId", context.get(DefaultPodcastService.PODCAST_EPISODE_CONTEXT_KEY));
			payload.put("segmentId", segmentId);
			NotificationEvents.notify(NotificationEvent.systemNotificationEventFor(mogulId, event,
					String.valueOf(segmentId), JsonUtils.write(payload)));
		} //
		catch (Exception e) {
			this.log.warn("couldn't emit a {} notification for podcast episode segment #{}",
					event.getClass().getSimpleName(), segmentId, e);
		}
	}

	@BatchMapping
	Map<Segment, Transcript> transcript(List<Segment> segments) {
		var mogul = this.mogulService.getCurrentMogul();
		var transcripts = this.transcriptService.transcripts(mogul.id(), segments);
		var map = new LinkedHashMap<Segment, Transcript>();
		for (var segment : segments)
			map.put(segment, transcripts.get(segment));
		return map;
	}

}
