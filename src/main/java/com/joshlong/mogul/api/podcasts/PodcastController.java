package com.joshlong.mogul.api.podcasts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.publication.PodcastEpisodePublisherPlugin;
import com.joshlong.mogul.api.publications.PublicationService;
import com.joshlong.mogul.api.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.http.MediaType;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Controller
class PodcastController {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<Long, PodcastEpisodeSseEmitter> episodeCompleteEventSseEmitters = new ConcurrentHashMap<>();

	private final ObjectMapper om;

	private final MogulService mogulService;

	private final PodcastService podcastService;

	private final Map<String, PodcastEpisodePublisherPlugin> plugins;

	private final PublicationService publicationService;

	private final Settings settings;

	PodcastController(MogulService mogulService, PodcastService podcastService,
			Map<String, PodcastEpisodePublisherPlugin> plugins, PublicationService publicationService,
			Settings settings, ObjectMapper om) {
		this.mogulService = mogulService;
		this.podcastService = podcastService;
		this.plugins = plugins;
		this.publicationService = publicationService;
		this.settings = settings;
		this.om = om;
	}

	@QueryMapping
	Collection<Episode> podcastEpisodesByPodcast(@Argument Long podcastId) {
		return this.podcastService.getEpisodesByPodcast(podcastId);
	}

	@MutationMapping
	boolean movePodcastEpisodeSegmentDown(@Argument Long episodeId, @Argument Long episodeSegmentId) {
		this.podcastService.movePodcastEpisodeSegmentDown(episodeId, episodeSegmentId);
		return true;
	}

	@MutationMapping
	boolean movePodcastEpisodeSegmentUp(@Argument Long episodeId, @Argument Long episodeSegmentId) {
		this.podcastService.movePodcastEpisodeSegmentUp(episodeId, episodeSegmentId);
		return true;
	}

	@MutationMapping
	Episode updatePodcastEpisode(@Argument Long episodeId, @Argument String title, @Argument String description) {
		return this.podcastService.updatePodcastEpisodeDraft(episodeId, title, description);
	}

	@QueryMapping
	Episode podcastEpisodeById(@Argument Long id) {
		return this.podcastService.getEpisodeById(id);
	}

	@MutationMapping
	boolean addPodcastEpisodeSegment(@Argument Long episodeId) {
		var mogul = this.mogulService.getCurrentMogul().id();

		this.podcastService.createEpisodeSegment(mogul, episodeId, "", 0);

		return true;
	}

	@SchemaMapping
	Collection<String> availablePlugins(Episode episode) {
		var mogul = mogulService.getCurrentMogul();
		var plugins = new HashSet<String>();
		for (var pluginName : this.plugins.keySet()) {
			var configuration = settings.getAllValuesByCategory(mogul.id(), pluginName);
			var plugin = this.plugins.get(pluginName);
			if (plugin.canPublish(configuration, episode)) {
				plugins.add(plugin.name());
			}
		}

		return plugins;
	}

	@SchemaMapping
	List<Segment> segments(Episode episode) {
		return this.podcastService.getEpisodeSegmentsByEpisode(episode.id());
	}

	@SchemaMapping
	long created(Episode episode) {
		return episode.created().getTime();
	}

	@QueryMapping
	Podcast podcastById(@Argument Long id) {
		return this.podcastService.getPodcastById(id);
	}

	@QueryMapping
	Collection<Podcast> podcasts() {
		return this.podcastService.getAllPodcastsByMogul(mogulService.getCurrentMogul().id());
	}

	@SchemaMapping
	Collection<Episode> episodes(Podcast podcast) {
		this.mogulService.assertAuthorizedMogul(podcast.mogulId());
		return this.podcastService.getEpisodesByPodcast(podcast.id());
	}

	@MutationMapping
	Long deletePodcastEpisode(@Argument Long id) {
		var ep = podcastService.getEpisodeById(id);
		this.mogulService.assertAuthorizedMogul(ep.podcast().mogulId());
		podcastService.deletePodcastEpisode(id);
		return id;
	}

	@MutationMapping
	Long deletePodcastEpisodeSegment(@Argument Long id) {
		this.podcastService.deletePodcastEpisodeSegment(id);
		return id;
	}

	@MutationMapping
	Long deletePodcast(@Argument Long id) {
		var podcast = this.podcastService.getPodcastById(id);
		Assert.notNull(podcast, "the podcast is null");
		var mogulId = podcast.mogulId();
		this.mogulService.assertAuthorizedMogul(mogulId);
		var podcasts = this.podcastService.getAllPodcastsByMogul(mogulId);
		Assert.state(!podcasts.isEmpty() && podcasts.size() - 1 > 0,
				"you must have at least one active, non-deleted podcast");
		this.podcastService.deletePodcast(podcast.id());
		return id;
	}

	@MutationMapping
	boolean publishPodcastEpisode(@Argument Long episodeId, @Argument String pluginName) {
		var episode = this.podcastService.getEpisodeById(episodeId);
		var mogul = episode.podcast().mogulId();
		var publication = this.publicationService.publish(mogul, episode, new HashMap<>(),
				this.plugins.get(pluginName));
		log.debug("finished publishing [{}] with plugin [{}] and got publication [{}] ", episode, pluginName,
				publication);
		return true;
	}

	@MutationMapping
	Podcast createPodcast(@Argument String title) {
		Assert.hasText(title, "the title for the podcast must be non-empty!");
		return podcastService.createPodcast(mogulService.getCurrentMogul().id(), title);
	}

	@MutationMapping
	Episode createPodcastEpisodeDraft(@Argument Long podcastId, @Argument String title, @Argument String description) {
		var currentMogulId = mogulService.getCurrentMogul().id();
		var podcast = podcastService.getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast is null!");
		mogulService.assertAuthorizedMogul(podcast.mogulId());
		return podcastService.createPodcastEpisodeDraft(currentMogulId, podcastId, title, description);
	}

	// if the user wants information about when an episode is ready to be published, we'll
	// setup an SSE stream here they can poll

	record PodcastEpisodeSseEmitter(Long podcastId, Long episodeId, SseEmitter sseEmitter) {

	}

	// todo remove this and make sure everything works as well with the new notifications
	// mechanism!
	@GetMapping("/podcasts/{podcastId}/episodes/{episodeId}/completions")
	SseEmitter streamPodcastEpisodeCompletionEvents(@PathVariable Long podcastId, @PathVariable Long episodeId) {
		log.debug("creating SSE watchdog for episode [{}]", episodeId);
		var peEmitter = new PodcastEpisodeSseEmitter(podcastId, episodeId, new SseEmitter());
		var episode = this.podcastService.getEpisodeById(episodeId);
		Assert.notNull(episode, "the episode is null");
		this.mogulService.assertAuthorizedMogul(episode.podcast().mogulId());
		Assert.state(episode.podcast().id().equals(podcastId),
				"the podcast specified and the actual podcast are not the same");
		Assert.state(episode.podcast().mogulId().equals(mogulService.getCurrentMogul().id()),
				"these are not the same Mogul");
		if (!this.episodeCompleteEventSseEmitters.containsKey(episodeId)) {
			this.episodeCompleteEventSseEmitters.put(episodeId, peEmitter);
		}
		log.debug("installing an SseEmitter for episode [{}]", episode);
		var cleanup = (Runnable) () -> {
			this.episodeCompleteEventSseEmitters.remove(episodeId);
			log.info("removing sse listener for episode [{}]", episodeId);
		};
		peEmitter.sseEmitter().onCompletion(cleanup);
		peEmitter.sseEmitter().onTimeout(cleanup);
		return peEmitter.sseEmitter();
	}

	@ApplicationModuleListener
	void broadcastPodcastEpisodeCompletionEventToClients(PodcastEpisodeCompletionEvent podcastEpisodeCompletionEvent) {
		var episode = podcastEpisodeCompletionEvent.episode();
		var id = episode.id();
		log.debug("going to send an event to the clients listening for episode [{}]", id);

		var emitter = this.episodeCompleteEventSseEmitters.get(id);

		if (null == emitter) {
			log.warn("could not find an emitter for episode [{}]", id);
			return;
		}

		try {
			var map = Map.of("id", id, "complete", episode.complete());
			var json = om.writeValueAsString(map);
			emitter.sseEmitter().send(json, MediaType.APPLICATION_JSON);
			log.debug("sent an event to clients listening for {}", episode);
		} //
		catch (Exception e) {
			log.warn("experienced an exception when trying to emit a podcast completed event via SSE for id # {}", id);
			emitter.sseEmitter().completeWithError(e);
		} //

	}

}
