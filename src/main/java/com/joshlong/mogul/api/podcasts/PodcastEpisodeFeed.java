package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.feeds.FeedTemplate;
import com.joshlong.mogul.api.feeds.SyndEntryMapper;
import com.joshlong.mogul.api.publications.PublicationService;
import com.joshlong.templates.MarkdownService;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final FeedTemplate template;

	private final PodcastService podcastService;

	private final PublicationService publicationService;

	private final MarkdownService markdownService;

	PodcastEpisodeFeed(FeedTemplate template, PodcastService podcastService, PublicationService publicationService,
			MarkdownService markdownService) {
		this.template = template;
		this.podcastService = podcastService;
		this.publicationService = publicationService;
		this.markdownService = markdownService;
	}

	@GetMapping("/public/feeds/moguls/{mogulId}/podcasts/{podcastId}/episodes.atom")
	ResponseEntity<String> podcastsFeed(@PathVariable long mogulId, @PathVariable long podcastId) {
		var podcast = this.podcastService.getPodcastById(podcastId);
		var episodes = this.podcastService.getPodcastEpisodesByPodcast(podcastId);
		Assert.state(podcast.mogulId().equals(mogulId), "the mogulId must match");
		var url = "/public/feeds/moguls/" + mogulId + "/podcasts/" + podcastId + "/episodes.atom";
		var title = podcast.title();
		var map = new HashMap<Long, String>();
		for (var e : episodes) {
			var publicationUrl = publicationUrl(e);
			if (StringUtils.hasText(publicationUrl)) {
				map.put(e.id(), publicationUrl);
			}
		}
		var publishedEpisodes = episodes.stream().filter(ep -> map.containsKey(ep.id())).toList();
		var syndEntryMapper = new EpisodeSyndEntryMapper(map);
		var feed = this.template.buildFeed(FeedTemplate.FeedType.ATOM_0_3, title, url, title, publishedEpisodes,
				syndEntryMapper);
		var render = this.template.render(feed);
		return ResponseEntity//
			.status(HttpStatusCode.valueOf(200))//
			.contentType(MediaType.APPLICATION_ATOM_XML)//
			.body(render);

	}

	private String publicationUrl(Episode ep) {
		var publications = this.publicationService.getPublicationsByPublicationKeyAndClass(ep.publicationKey(),
				Episode.class);
		if (ep.complete() && !publications.isEmpty())
			return publications//
				.stream()//
				.sorted(((Comparator<Publication>) (o1, o2) -> {
					if (o1 != null && o2 != null) {
						if (o1.published() != null && o2.published() != null)
							return o1.published().compareTo(o2.published());
						if (o1.created() != null && o2.created() != null)
							return o1.created().compareTo(o2.created());
					}
					return 0;
				})//
					.reversed()//
				)//
				.toList()
				.getFirst()
				.url();
		return null;
	}

	private class EpisodeSyndEntryMapper implements SyndEntryMapper<Episode> {

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
			description.setType(MediaType.TEXT_PLAIN_VALUE);
			description.setValue(episode.description());

			// var markdownDescription = new SyndContentImpl();
			// markdownDescription.setType(MediaType.TEXT_HTML_VALUE);
			// markdownDescription.setValue(markdownDescription(episode.description()));
			//
			entry.setDescription(description);
			return entry;

		}

		private String markdownDescription(String description) {
			try {
				return markdownService.convertMarkdownTemplateToHtml(description);
			}
			catch (Throwable throwable) {
				log.warn("couldn't transcode the following " + "string into Markdown: {}", description + "");
			}
			return description;
		}

	}

}
