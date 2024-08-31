package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.podcasts.publication.PodcastEpisodePublisherPlugin;
import com.joshlong.mogul.api.publications.PublicationService;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.graphql.data.method.annotation.*;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@Controller
@RegisterReflectionForBinding(Map.class)
class PodcastController {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ApplicationEventPublisher publisher;

	private final MogulService mogulService;

	private final PodcastService podcastService;

	private final Map<String, PodcastEpisodePublisherPlugin> plugins;

	private final PublicationService publicationService;

	private final Settings settings;

	PodcastController(ApplicationEventPublisher publisher, MogulService mogulService, PodcastService podcastService,
			Map<String, PodcastEpisodePublisherPlugin> plugins, PublicationService publicationService,
			Settings settings) {
		this.publisher = publisher;
		this.mogulService = mogulService;
		this.podcastService = podcastService;
		this.plugins = plugins;
		this.publicationService = publicationService;
		this.settings = settings;
	}

	@QueryMapping
	Collection<Episode> podcastEpisodesByPodcast(@Argument Long podcastId) {
		return this.podcastService.getEpisodesByPodcast(podcastId);
	}

	@SchemaMapping
	Collection<Publication> publications(Episode episode) {
		var episodeKeyForLogging = episode.id() + "/" + episode.title();
		var publications = this.publicationService.getPublicationsByPublicationKeyAndClass(episode.publicationKey(),
				episode.getClass());
		if (!publications.isEmpty()) {
			this.log.debug("good news! there are " + "publications for episode {}", episodeKeyForLogging);
		}
		return publications;
		// var newPublications = new ArrayList<Map<String, Object>>();
		// for (var p : publications) {
		// var defaultedValues = Map.of("id", p.id(), "mogulId", p.mogulId(), "plugin",
		// p.plugin(), "created",
		// p.created().getTime());
		// var all = new HashMap<String, Object>(defaultedValues);
		// if (p.published() != null)
		// all.put("published", p.published().getTime());
		// if (StringUtils.hasText(p.url()))
		// all.put("url", p.url());
		// newPublications.add(all);
		// }
		// if (!newPublications.isEmpty())
		// this.log.debug("returning {} publications for episode {}",
		// newPublications.size(), episodeKeyForLogging);
		// return newPublications;
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

	@BatchMapping
	Map<Episode, Collection<String>> availablePlugins(List<Episode> episodes) {
		var mogul = this.mogulService.getCurrentMogul();
		var mapOfEpisodesToValidPlugins = new HashMap<Episode, Collection<String>>();
		for (var pluginEntry : this.plugins.entrySet()) {
			var pluginName = pluginEntry.getKey();
			var plugin = pluginEntry.getValue();
			var configuration = this.settings.getAllValuesByCategory(mogul.id(), pluginName);
			for (var episode : episodes) {
				var pluginNamesForEpisode = mapOfEpisodesToValidPlugins.computeIfAbsent(episode,
						e -> new ArrayList<>());
				if (plugin.canPublish(configuration, episode)) {
					pluginNamesForEpisode.add(plugin.name());
				} //
				else {
					log.trace("can not publish with plugin {} for episode #{} with title {}", plugin.name(),
							episode.id(), episode.title());
				}
			}
		}
		return mapOfEpisodesToValidPlugins;
	}

	@BatchMapping
	Map<Episode, List<Segment>> segments(List<Episode> episodes) {
		var byId = this.podcastService
			.getEpisodeSegmentsByEpisodes(episodes.stream().map(Episode::id).collect(Collectors.toSet()));
		var allEpisodes = this.podcastService.getAllEpisodesByIds(episodes.stream().map(Episode::id).toList());
		var map = new HashMap<Episode, List<Segment>>();
		for (var e : allEpisodes)
			map.put(e, byId.get(e.id()));
		return map;
	}

	@SchemaMapping
	long created(Podcast podcast) {
		return podcast.created().getTime();
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
		var currentMogul = mogulService.getCurrentMogul();
		this.log.debug("attempting to read the podcasts associated with mogul #{} - {}", currentMogul.id(),
				currentMogul.username());
		return this.podcastService.getAllPodcastsByMogul(currentMogul.id());
	}

	@SchemaMapping
	Collection<Episode> episodes(Podcast podcast) {
		this.mogulService.assertAuthorizedMogul(podcast.mogulId());
		var episodesByPodcast = this.podcastService.getEpisodesByPodcast(podcast.id());
		this.log.debug("episodes by podcast #{} {}", podcast.id(), podcast.title());
		return episodesByPodcast;
	}

	@MutationMapping
	Long deletePodcastEpisode(@Argument Long id) {
		// var ep = this.podcastService.getEpisodeById(id);
		// this.mogulService.assertAuthorizedMogul(ep.podcast().mogulId());
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
	Publication unpublishPodcastEpisodePublication(@Argument Long publicationId) {
		log.debug("going to unpublish the publication with id # {}", publicationId);
		var publicationById = this.publicationService.getPublicationById(publicationId);
		Assert.notNull(publicationById, "the publication should not be null");
		var plugin = this.plugins.get(publicationById.plugin());
		Assert.notNull(plugin, "you must specify an active plugin");
		return this.publicationService.unpublish(publicationById.mogulId(), publicationById, plugin);
	}

	@SchemaMapping
	long created(Publication publication) {
		return publication.created().getTime();
	}

	@SchemaMapping
	Long published(Publication publication) {
		return publication.published() != null ? publication.published().getTime() : null;
	}

	// created: Float
	// published: Float

	// todo return a Publication from this endpoint

	@MutationMapping
	Publication publishPodcastEpisode(@Argument Long episodeId, @Argument String pluginName) {
		var episode = this.podcastService.getEpisodeById(episodeId);
		var mogul = this.podcastService.getPodcastById(episode.podcastId()).mogulId();
		var contextAndSettings = new HashMap<String, String>();
		var publication = this.publicationService.publish(mogul, episode, contextAndSettings,
				this.plugins.get(pluginName));
		log.debug("finished publishing [{}] with plugin [{}] and got publication [{}] ",
				"#" + episode.id() + "/" + episode.title(), pluginName, publication);
		return publication;
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
			var json = JsonUtils.write(map);
			this.log.debug("json: {}", json);
			var evt = NotificationEvent.notificationEventFor(podcastEpisodeCompletionEvent.mogulId(),
					podcastEpisodeCompletionEvent, Long.toString(episode.id()), json, false, false);
			this.publisher.publishEvent(evt);
			this.log.debug("sent an event to clients listening for {}", episode);
		} //
		catch (Exception e) {
			this.log.warn("experienced an exception when trying to emit a podcast completed event via SSE for id # {}",
					id);
		} //

	}

}
