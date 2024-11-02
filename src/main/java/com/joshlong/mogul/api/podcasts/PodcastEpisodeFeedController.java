package com.joshlong.mogul.api.podcasts;

import com.joshlong.feed.FeedTemplate;
import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.publications.PublicationService;
import com.rometools.rome.feed.WireFeed;
import com.rometools.rome.feed.atom.Content;
import com.rometools.rome.feed.atom.Entry;
import com.rometools.rome.feed.atom.Feed;
import com.rometools.rome.feed.synd.*;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

@Controller
@ResponseBody
class PodcastEpisodeFeedController {

	private static final String PODCAST_FEED_URL = "/public/feeds/moguls/{mogulId}/podcasts/{podcastId}/episodes.atom";

	@GetMapping(PODCAST_FEED_URL)
	String feed() {
		return "hello world!";
	}
}


class PodcastEpisodeFeed {


	
	private final ManagedFileService managedFileService;
	private final PodcastService podcastService;
	private final PublicationService publicationService;
	private final MogulService mogulService;


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

	PodcastEpisodeFeed(ManagedFileService managedFileService, PodcastService podcastService, PublicationService publicationService, MogulService mogulService) {
		this.managedFileService = managedFileService;
		this.podcastService = podcastService;
		this.publicationService = publicationService;
		this.mogulService = mogulService;
	}


	private final Logger log =  LoggerFactory.getLogger(getClass());

	private SyndEntry entry(String mogulName, String publishedUrl, Episode episode) {

		var se = new SyndEntryImpl();
		
		var sc = new SyndContentImpl();
		sc.setType(MediaType.TEXT_PLAIN_VALUE);
		sc.setValue(episode.description());
		se.setDescription( sc );


		//
		var customNs = Namespace.getNamespace("custom", "http://mycompany.com/customdata");

		// Create elements
		var metadata = new Element("metadata", customNs);
		Element correlationId = new Element("correlationId", customNs);
		correlationId.setText("ABC123");
		metadata.addContent(correlationId);


		// Add to entry
		se.getForeignMarkup().add(metadata);
		//

		se.setUpdatedDate( episode.created());
		se.setPublishedDate(episode.created());
		se.setAuthor(mogulName);
		se.setTitle(episode.title());
		se.setUri(publishedUrl);
		var description = new Content();
		description.setType(MediaType.TEXT_PLAIN_VALUE);
		description.setValue(episode.description());

		var graphicId = episode.producedGraphic().id();
		var url = "/api" + managedFileService.getPublicUrlForManagedFile(graphicId);

		var image = new SyndLinkImpl();
		image.setHref(url);
		image.setRel("enclosure");
		image.setType(episode.graphic().contentType());

		se.getLinks().add(image);

		var category = new SyndCategoryImpl();
		category.setName("a");
		category.setLabel("b");
		se.setCategories(List.of(category));
 
		return se; 
	}

	Feed podcastsFeed(long mogulId, long podcastId) {

		var mogul = this.mogulService.getMogulById(mogulId);
		var podcast = this.podcastService.getPodcastById(podcastId);
		var episodes = this.podcastService.getPodcastEpisodesByPodcast(podcastId);

		var syndFeed = new SyndFeedImpl();
		syndFeed.setFeedType(FeedTemplate.FeedType.ATOM_0_3.value());
		syndFeed.setTitle(podcast.title());
		
		var author = mogul.givenName() + " " + mogul.familyName();
		syndFeed.setAuthor(author);
		


		
		var entries = new ArrayList<SyndEntry>();
		

		var map = new HashMap<Long, String>();
		for (var e : episodes) {
			var publicationUrl = publicationUrl(e);
			if (StringUtils.hasText(publicationUrl)) {
				map.put(e.id(), publicationUrl);
			}
		}
		System.out.println("map " + map);

		for (var published : episodes.stream().filter(e -> map.containsKey(e.id())).toList()) {
			System.out.println("published " + published);
			var syndEntry = entry(author, map.get(published.id()), published);
			
			entries.add(syndEntry);
		}


 			
		if (syndFeed.createWireFeed() instanceof Feed feed)
		{
			 log.info("we have a valid feed, so adding {} entries" , entries.size());
			var es=entries.stream().map ( e-> (Entry)e.getWireEntry()).toList();
			 feed. setEntries(  es);
			return feed;
		}
		

		 throw new IllegalArgumentException("we should never get to this point!") ;
		
	}

	private String publicationUrl(Episode ep) {
		var publications = this.publicationService.getPublicationsByPublicationKeyAndClass(ep.publicationKey(), Episode.class);
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

}