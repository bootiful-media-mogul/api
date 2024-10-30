package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.feeds.FeedTemplate;
import com.joshlong.mogul.api.feeds.SyndEntryMapper;
import com.joshlong.mogul.api.publications.PublicationService;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@Controller
@ResponseBody
class PodcastEpisodeFeed {

	private final FeedTemplate template;

	private final PodcastService podcastService;

	private final PublicationService publicationService;

	PodcastEpisodeFeed(FeedTemplate template, PodcastService podcastService, PublicationService publicationService) {
		this.template = template;
		this.podcastService = podcastService;
		this.publicationService = publicationService;
	}

	@GetMapping("/feeds/moguls/{mogulId}/podcasts/{podcastId}/episodes.atom")
	ResponseEntity<String> podcastsFeed(@PathVariable long mogulId, @PathVariable long podcastId) {
		var podcast = this.podcastService.getPodcastById(podcastId);
		Assert.state(podcast.mogulId().equals(mogulId), "the mogulId must match");
		var episodes = podcast.episodes();
		var url = "/feeds/moguls/" + mogulId + "/podcasts/" + podcastId + "/episodes.atom";
		var title = podcast.title();
		var map = new HashMap<Long, String>();
		for (var e : episodes) {
			var publicationUrl = publicationUrl(e);
			if (StringUtils.hasText(publicationUrl)) {
				map.put(e.id(), publicationUrl);
			}
		}
		var pubishedEpisodes = episodes.stream().filter(ep -> map.containsKey(ep.id())).toList();
		var syndEntryMapper = new EpisodeSyndEntryMapper(map);
		var feed = this.template.buildFeed(FeedTemplate.FeedType.ATOM_0_3, title, url, title, pubishedEpisodes,
				syndEntryMapper);
		var render = this.template.render(feed);
		return ResponseEntity.status(HttpStatusCode.valueOf(200))
			.contentType(MediaType.APPLICATION_ATOM_XML)
			.body(render);

	}

	private String publicationUrl(Episode ep) {
		var publications = this.publicationService.getPublicationsByPublicationKeyAndClass(ep.publicationKey(),
				Episode.class);
		if (ep.complete() && !publications.isEmpty())
			return publications.stream()
				.sorted(Comparator.comparing(Publication::published).reversed())
				.toList()
				.getFirst()
				.url();
		return null;
	}

	static class EpisodeSyndEntryMapper implements SyndEntryMapper<Episode> {

		private final Map<Long, String> urls;

		EpisodeSyndEntryMapper(Map<Long, String> urls) {
			this.urls = urls;
		}

		@Override
		public SyndEntry map(Episode episode) throws Exception {

			var entry = new SyndEntryImpl();
			entry.setTitle(episode.title());
			entry.setLink(urls.get(episode.id()));
			entry.setPublishedDate(episode.created());

			var description = new SyndContentImpl();
			description.setType("text/plain");
			description.setValue(episode.description());

			entry.setDescription(description);
			return entry;

		}

	}

}
