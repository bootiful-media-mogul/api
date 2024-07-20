package com.joshlong.mogul.api.podcasts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.podcasts.publication.PodcastEpisodePublisherPlugin;
import com.joshlong.mogul.api.publications.PublicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

import java.util.*;

@Controller
@RegisterReflectionForBinding(Map.class)
class PodcastController {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ApplicationEventPublisher publisher;

	private final ObjectMapper om;

	private final MogulService mogulService;

	private final PodcastService podcastService;

	private final Map<String, PodcastEpisodePublisherPlugin> plugins;

	private final PublicationService publicationService;

	private final Settings settings;

	PodcastController(ApplicationEventPublisher publisher, MogulService mogulService, PodcastService podcastService,
			Map<String, PodcastEpisodePublisherPlugin> plugins, PublicationService publicationService,
			Settings settings, ObjectMapper om) {
		this.publisher = publisher;
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
		var mogul = this.mogulService.getCurrentMogul();
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
		var ep = this.podcastService.getEpisodeById(id);
		this.mogulService.assertAuthorizedMogul(ep.podcast().mogulId());
		this.podcastService.deletePodcastEpisode(id);
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
		return this.podcastService.createPodcast(this.mogulService.getCurrentMogul().id(), title);
	}

	@MutationMapping
	Episode createPodcastEpisodeDraft(@Argument Long podcastId, @Argument String title, @Argument String description) {
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		var podcast = this.podcastService.getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast is null!");
		this.mogulService.assertAuthorizedMogul(podcast.mogulId());
		return this.podcastService.createPodcastEpisodeDraft(currentMogulId, podcastId, title, description);
	}

	@ApplicationModuleListener
	void broadcastPodcastEpisodeCompletionEventToClients(PodcastEpisodeCompletionEvent podcastEpisodeCompletionEvent) {
		var episode = podcastEpisodeCompletionEvent.episode();
		var id = episode.id();
		this.log.debug("going to send an event to the clients listening for episode [{}]", id);
		try {
			var map = Map.of("episodeId", id, "complete", episode.complete());
			var json = this.om.writeValueAsString(map);
			var evt = NotificationEvent.notificationEventFor(
					podcastEpisodeCompletionEvent.episode().podcast().mogulId(), podcastEpisodeCompletionEvent,
					Long.toString(episode.id()), json, false, false);
			this.publisher.publishEvent(evt);
			this.log.debug("sent an event to clients listening for {}", episode);
		} //
		catch (Exception e) {
			this.log.warn("experienced an exception when trying to emit a podcast completed event via SSE for id # {}", id);
		} //

	}

}
