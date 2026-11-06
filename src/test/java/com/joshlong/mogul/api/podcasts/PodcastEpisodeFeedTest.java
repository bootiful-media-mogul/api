package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.feeds.Feeds;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.publications.PublicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class PodcastEpisodeFeedTest {

	private final AtomicLong counter = new AtomicLong(0);

	private ManagedFileService managedFileService;

	private PodcastService podcastService;

	private MogulService mogulService;

	private PublicationService publicationService;

	@BeforeEach
	void setup() {
		publicationService = mock(PublicationService.class);
		mogulService = mock(MogulService.class);
		managedFileService = mock(ManagedFileService.class);
		podcastService = mock(PodcastService.class);
	}

	@Test
	void feed() throws Exception {
		var mogulId = 1L;
		var podcastId = 1L;
		var mogul = new Mogul(mogulId, "jlong", "josh@joshlong.com", "clientId", "Josh", "Long", new Date(), "UTC");
		var podcast = new Podcast(mogul.id(), podcastId, "the title", new Date());
		var sharedEverywhere = nextEpisode(mogulId, podcastId, true);
		var blogged = nextEpisode(mogulId, podcastId, true);
		var neverPublished = nextEpisode(mogulId, podcastId, true);
		var incomplete = nextEpisode(mogulId, podcastId, false);
		var episodes = List.of(sharedEverywhere, blogged, neverPublished, incomplete);

		when(this.managedFileService.getPublicUrlForManagedFile(Mockito.anyLong()))
			.thenAnswer((Answer<String>) invocation -> "/public/managedfiles/" + invocation.getArguments()[0]);
		when(this.mogulService.getMogulById(mogulId)).thenReturn(mogul);
		when(this.podcastService.getPodcastById(podcastId)).thenReturn(podcast);
		when(this.podcastService.getPodcastEpisodesByPodcast(podcastId, true)).thenReturn(episodes);

		var publications = new HashMap<Long, Collection<Publication>>();
		// the social share comes last, and has one outcome for each network: it mustn't
		// hide the episode, or win out over the podbean link.
		publications.put(sharedEverywhere.id(),
				List.of(publication("podcastEpisodeAyrshare", 3, Publication.State.PUBLISHED, "https://bsky.app/post/1",
						"https://www.linkedin.com/post/1"),
						publication("podcastEpisodeToBlogPost", 2, Publication.State.PUBLISHED,
								"https://studio.mogul.tools/blogs/1/posts/1"),
						publication("podbean", 1, Publication.State.PUBLISHED,
								"https://bootifulpodcast.podbean.com/e/shared-everywhere/"),
						publication("podbean", 0, Publication.State.UNPUBLISHED,
								"https://bootifulpodcast.podbean.com/e/gone/")));
		publications.put(blogged.id(), List.of(publication("podcastEpisodeToBlogPost", 1, Publication.State.PUBLISHED,
				"https://studio.mogul.tools/blogs/1/posts/2")));
		publications.put(neverPublished.id(), List.of(publication("podbean", 1, Publication.State.DRAFT)));
		publications.put(incomplete.id(), List.of(publication("podbean", 1, Publication.State.PUBLISHED,
				"https://bootifulpodcast.podbean.com/e/incomplete/")));
		when(this.publicationService.getPublicationsByPublicationKeysAndClass(Mockito.anyCollection(),
				Mockito.eq(Episode.class)))
			.thenReturn(publications);

		var controller = new PodcastEpisodeFeedController(this.podcastService, this.publicationService,
				this.mogulService, new Feeds("mogul-feeds", "http://api.mogul.tools"), this.managedFileService);
		var feed = controller.feed(new MockHttpServletRequest("GET", "/"), mogulId, podcastId);

		var links = links(feed);
		assertThat(links).containsExactly("https://bootifulpodcast.podbean.com/e/shared-everywhere/",
				"https://studio.mogul.tools/blogs/1/posts/2");
	}

	@Test
	void browsersOnOtherSitesCanReadTheFeed() throws Exception {
		var mogul = new Mogul(1L, "jlong", "josh@joshlong.com", "clientId", "Josh", "Long", new Date(), "UTC");
		when(this.mogulService.getMogulById(1L)).thenReturn(mogul);
		when(this.podcastService.getPodcastById(1L)).thenReturn(new Podcast(1L, 1L, "the title", new Date()));
		when(this.podcastService.getPodcastEpisodesByPodcast(1L, true)).thenReturn(List.of());
		when(this.publicationService.getPublicationsByPublicationKeysAndClass(Mockito.anyCollection(),
				Mockito.eq(Episode.class)))
			.thenReturn(Map.of());
		var controller = new PodcastEpisodeFeedController(this.podcastService, this.publicationService,
				this.mogulService, new Feeds("mogul-feeds", "http://api.mogul.tools"), this.managedFileService);
		MockMvcBuilders.standaloneSetup(controller)
			.build()
			.perform(get("/public/feeds/moguls/1/podcasts/1/episodes.atom").header(HttpHeaders.ORIGIN,
					"https://bootifulpodcast.fm"))
			.andExpect(status().isOk())
			.andExpect(header().string(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*"));
	}

	private Publication publication(String plugin, int minutesAgo, Publication.State state, String... urls)
			throws Exception {
		var when = new Date(System.currentTimeMillis() - minutesAgo * 60_000L);
		var outcomes = new ArrayList<Publication.Outcome>();
		for (var url : urls)
			outcomes.add(new Publication.Outcome(0, when, true, URI.create(url).toURL(), plugin, null, null));
		return new Publication(1L, this.counter.incrementAndGet(), plugin, when, when, Map.of(), "", Episode.class,
				state, outcomes, null);
	}

	// the entry links that aren't the thumbnail enclosure, in order
	private static List<String> links(String feed) throws Exception {
		var factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		var document = factory.newDocumentBuilder().parse(new InputSource(new StringReader(feed)));
		var entries = document.getElementsByTagNameNS("http://www.w3.org/2005/Atom", "entry");
		var links = new ArrayList<String>();
		for (var i = 0; i < entries.getLength(); i++) {
			var entryLinks = ((Element) entries.item(i)).getElementsByTagNameNS("http://www.w3.org/2005/Atom", "link");
			for (var j = 0; j < entryLinks.getLength(); j++) {
				var link = (Element) entryLinks.item(j);
				if (!link.hasAttribute("rel"))
					links.add(link.getAttribute("href"));
			}
		}
		return links;
	}

	private Episode nextEpisode(long mogulId, long podcastId, boolean complete) throws Exception {
		var nextId = counter.incrementAndGet();
		var producedAudio = nextManagedFile(mogulId, true, CommonMediaTypes.MP3.toString());
		var graphic = nextManagedFile(mogulId, true, MediaType.IMAGE_JPEG_VALUE);
		var producedGraphic = nextManagedFile(mogulId, true, MediaType.IMAGE_JPEG_VALUE);
		return new Episode(nextId, podcastId, "the title for episode " + nextId,
				"the description for episode " + nextId, new Date(), graphic, producedGraphic, producedAudio, complete,
				new Date(), new Date());
	}

	private ManagedFile nextManagedFile(long mogulId, boolean visible, String contentType) throws Exception {
		var mf = Mockito.mock(ManagedFile.class);
		when(mf.id()).thenReturn(counter.incrementAndGet());
		when(mf.bucket()).thenReturn("bucket");
		when(mf.filename()).thenReturn("filename");
		when(mf.folder()).thenReturn("folder");
		when(mf.contentType()).thenReturn(contentType);
		when(mf.mogulId()).thenReturn(mogulId);
		when(mf.storageFilename()).thenReturn("storageFilename." + contentType.split("/")[1]);
		when(mf.visible()).thenReturn(visible);
		when(mf.created()).thenReturn(new Date());
		when(mf.size()).thenReturn(100L);
		var file = File.createTempFile("test", "");
		file.deleteOnExit();
		when(mf.uniqueLocalFile()).thenReturn(file);
		return mf;
	}

}