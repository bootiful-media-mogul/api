package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.feeds.Entry;
import com.joshlong.mogul.api.feeds.EntryMapper;
import com.joshlong.mogul.api.feeds.Feeds;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.publications.PublicationService;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@Controller
@ResponseBody
class PodcastEpisodeFeedController {

	private static final String PODCAST_FEED_URL = "/public/feeds/moguls/{mogulId}/podcasts/{podcastId}/episodes.atom";

	private final PodcastService podcastService;

	private final PublicationService publicationService;

	private final MogulService mogulService;

	private final Feeds feeds;

	PodcastEpisodeFeedController(PodcastService podcastService, PublicationService publicationService,
			MogulService mogulService, Feeds feeds) {
		this.podcastService = podcastService;
		this.publicationService = publicationService;
		this.mogulService = mogulService;
		this.feeds = feeds;
	}

	@GetMapping(PODCAST_FEED_URL)
	String feed(@PathVariable long mogulId, @PathVariable long podcastId)
			throws TransformerException, IOException, ParserConfigurationException {
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

		//
		var map = new HashMap<Long, String>();
		for (var e : episodes) {
			var publicationUrl = publicationUrl(e);
			if (StringUtils.hasText(publicationUrl)) {
				map.put(e.id(), publicationUrl);
			}
		}
		var publishedEpisodes = episodes.stream().filter(ep -> map.containsKey(ep.id())).toList();
		//

		var mapper = new PodcastEpisodeEntryMapper(map);

		return feeds.createMogulAtomFeed(podcast.title(), url, podcast.created().toInstant(), author,
				Long.toString(podcastId), publishedEpisodes, mapper);

	}

	public static class PodcastEpisodeEntryMapper implements EntryMapper<Episode> {

		private final Map<Long, String> urls;

		PodcastEpisodeEntryMapper(Map<Long, String> urls) {
			this.urls = urls;
		}

		@Override
		public Entry map(Episode episode) throws Exception {
			return new Entry(Long.toString(episode.id()), episode.created().toInstant(), episode.title(),
					this.urls.get(episode.id()), episode.description(),
					Map.of("uuid", Long.toString(episode.id()), "this", "that"));
		}

	}

	private String publicationUrl(Episode ep) {
		var publications = this.publicationService.getPublicationsByPublicationKeyAndClass(ep.publicationKey(),
				Episode.class);
		if (ep.complete() && !publications.isEmpty()) {

			return publications//
				.stream()//
				.sorted(publicationComparator)//
				.toList()
				.getFirst()
				.url();
		}
		return null;
	}

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

}
