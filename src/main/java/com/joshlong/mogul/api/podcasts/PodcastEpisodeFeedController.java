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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.adapter.ForwardedHeaderTransformer;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
class FeedConfiguration {

	@Bean
	ForwardedHeaderTransformer forwardedHeaderTransformer() {
		return new ForwardedHeaderTransformer();
	}

}

@Controller
@ResponseBody
class PodcastEpisodeFeedController {

	private static final String PODCAST_FEED_URL = "/public/feeds/moguls/{mogulId}/podcasts/{podcastId}/episodes.atom";

	private final Comparator<Publication> publicationComparator = ((Comparator<Publication>) (o1, o2) -> {
		if (o1 != null && o2 != null) {
			if (o1.published() != null && o2.published() != null)
				return o1.published().compareTo(o2.published());
			if (o1.created() != null && o2.created() != null)
				return o1.created().compareTo(o2.created());
		}
		return 0;
	})//
		.reversed();

	private final PodcastService podcastService;

	private final PublicationService publicationService;

	private final MogulService mogulService;

	private final Feeds feeds;

	private final ManagedFileService managedFileService;

	PodcastEpisodeFeedController(PodcastService podcastService, PublicationService publicationService,
			MogulService mogulService, Feeds feeds, ManagedFileService managedFileService) {
		this.podcastService = podcastService;
		this.publicationService = publicationService;
		this.mogulService = mogulService;
		this.feeds = feeds;
		this.managedFileService = managedFileService;
	}

	@GetMapping(PODCAST_FEED_URL)
	String feed(HttpServletRequest request, @PathVariable long mogulId, @PathVariable long podcastId)
			throws TransformerException, IOException, ParserConfigurationException {

		var serverRequest = new ServletServerHttpRequest(request);

		System.out.println(serverRequest);

		var mogul = this.mogulService.getMogulById(mogulId);
		var podcast = this.podcastService.getPodcastById(podcastId);
		var episodes = this.podcastService.getPodcastEpisodesByPodcast(podcastId);
		var author = mogul.givenName() + ' ' + mogul.familyName();
		var url = PODCAST_FEED_URL;
		for (var k : Map.of("mogulId", mogulId, "podcastId", podcastId).entrySet()) {
			var key = "{" + k.getKey() + "}";
			if (url.contains(key)) {
				url = url.replace(key, Long.toString(k.getValue()));
			}
		}
		var episodeIdToPublicationUrl = new HashMap<Long, String>();
		for (var e : episodes) {
			var publicationUrl = publicationUrl(e);
			if (StringUtils.hasText(publicationUrl)) {
				episodeIdToPublicationUrl.put(e.id(), publicationUrl);
			}
		}
		var publishedEpisodes = episodes.stream().filter(ep -> episodeIdToPublicationUrl.containsKey(ep.id())).toList();
		var mapper = new PodcastEpisodeEntryMapper(episodeIdToPublicationUrl);
		return this.feeds.createMogulAtomFeed(podcast.title(), url, podcast.created().toInstant(), author,
				longToUuid(podcastId).toString(), publishedEpisodes, mapper);
	}

	private static UUID longToUuid(long id) {
		// Split the long into most and least significant bits
		return new UUID(0, id); // Uses 0 for most significant bits
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
					this.urls.get(episode.id()), episode.description(),
					Map.of("uuid", Long.toString(episode.id()), "this", "that"), img);
		}

	}

	private String publicationUrl(Episode episode) {
		var publications = this.publicationService.getPublicationsByPublicationKeyAndClass(episode.publicationKey(),
				Episode.class);
		if (episode.complete() && !publications.isEmpty()) {
			return publications//
				.stream()//
				.sorted(this.publicationComparator)//
				.toList()
				.getFirst()
				.url();
		}
		return null;
	}

}
