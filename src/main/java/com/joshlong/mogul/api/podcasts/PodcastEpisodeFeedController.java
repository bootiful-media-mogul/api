package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.feeds.Entry;
import com.joshlong.mogul.api.feeds.EntryMapper;
import com.joshlong.mogul.api.feeds.Feeds;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.publications.PublicationService;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Controller
@ResponseBody
class PodcastEpisodeFeedController {

	private static final String PODCAST_FEED_URL = "/public/feeds/moguls/{mogulId}/podcasts/{podcastId}/episodes.atom";

	/**
	 * the plugins whose links make the best home for an episode, best first. an episode
	 * is usually published to podbean, then to a blog post, then shared on social media,
	 * and the social shares - one outcome for each network - are the least useful link of
	 * all, so any plugin not named here comes last. these are the names stored in each
	 * publication's {@code plugin} column.
	 */
	private static final List<String> PREFERRED_PLUGINS = List.of("podbean", "podcastEpisodeToBlogPost");

	private final Comparator<Publication> publicationComparator = Comparator
		.comparingInt((Publication publication) -> preference(publication.plugin()))
		.thenComparing(Publication::created, Comparator.nullsLast(Comparator.reverseOrder()));

	private final PodcastService podcastService;

	private final PublicationService publicationService;

	private final MogulService mogulService;

	private final Feeds feeds;

	private final ManagedFileService managedFileService;

	private final Logger log = LoggerFactory.getLogger(getClass());

	PodcastEpisodeFeedController(PodcastService podcastService, PublicationService publicationService,
			MogulService mogulService, Feeds feeds, ManagedFileService managedFileService) {
		this.podcastService = podcastService;
		this.publicationService = publicationService;
		this.mogulService = mogulService;
		this.feeds = feeds;
		this.managedFileService = managedFileService;
	}

	private static UUID longToUuid(long id) {
		// Split the long into most and least significant bits
		return new UUID(0, id); // Uses 0 for most significant bits
	}

	@GetMapping(PODCAST_FEED_URL)
	String feed(HttpServletRequest request, @PathVariable long mogulId, @PathVariable long podcastId)
			throws TransformerException, IOException, ParserConfigurationException {

		var serverRequest = new ServletServerHttpRequest(request);

		log.info(serverRequest.toString());

		var mogul = this.mogulService.getMogulById(mogulId);
		var podcast = this.podcastService.getPodcastById(podcastId);

		var episodes = this.podcastService.getPodcastEpisodesByPodcast(podcastId, true);
		var author = mogul.givenName() + ' ' + mogul.familyName();
		var url = PODCAST_FEED_URL;
		for (var k : Map.of("mogulId", mogulId, "podcastId", podcastId).entrySet()) {
			var key = "{" + k.getKey() + "}";
			if (url.contains(key)) {
				url = url.replace(key, Long.toString(k.getValue()));
			}
		}
		var publications = this.publicationService.getPublicationsByPublicationKeysAndClass(
				episodes.stream().map(Episode::publishableId).toList(), Episode.class);
		var episodeIdToPublicationUrl = new HashMap<Long, String>();
		for (var e : episodes) {
			var publicationUrl = e.complete() ? publicationUrl(publications.getOrDefault(e.publishableId(), List.of()))
					: null;
			if (StringUtils.hasText(publicationUrl)) {
				episodeIdToPublicationUrl.put(e.id(), publicationUrl);
			}
		}
		var publishedEpisodes = episodes.stream().filter(ep -> episodeIdToPublicationUrl.containsKey(ep.id())).toList();
		var mapper = new PodcastEpisodeEntryMapper(episodeIdToPublicationUrl);
		return this.feeds.createMogulAtomFeed(podcast.title(), url, podcast.created().toInstant(), author,
				longToUuid(podcastId).toString(), publishedEpisodes, mapper);
	}

	private static int preference(String plugin) {
		var index = PREFERRED_PLUGINS.indexOf(plugin);
		return index == -1 ? PREFERRED_PLUGINS.size() : index;
	}

	private String publicationUrl(Collection<Publication> publications) {
		return publications.stream()
			.filter(publication -> publication.state() == Publication.State.PUBLISHED)
			.sorted(this.publicationComparator)
			.flatMap(publication -> publication.outcomes().stream())
			.filter(outcome -> outcome.success() && outcome.url() != null)
			.map(outcome -> outcome.url().toString())
			.findFirst()
			.orElse(null);
	}

	private class PodcastEpisodeEntryMapper implements EntryMapper<Episode> {

		private final Map<Long, String> urls;

		private PodcastEpisodeEntryMapper(Map<Long, String> urls) {
			this.urls = urls;
		}

		@Override
		public Entry map(Episode episode) {
			var graphicManagedFile = episode.producedGraphic();
			var urlForManagedFile = managedFileService.getPublicUrlForManagedFile(graphicManagedFile.id());
			var img = new Entry.Image(urlForManagedFile, graphicManagedFile.size(), graphicManagedFile.contentType());
			return new Entry(longToUuid(episode.id()).toString(), episode.created().toInstant(), episode.title(),
					this.urls.get(episode.id()), episode.description(), Map.of("id", Long.toString(episode.id())), img);
		}

	}

}
