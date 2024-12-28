package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.publications.PublicationService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.http.MediaType;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PodcastEpisodeFeedTest {

	private final AtomicLong counter = new AtomicLong(0);

	private ManagedFileService managedFileService;

	private PodcastService podcastService;

	private MogulService mogulService;

	private PublicationService publicationService;

	@BeforeEach
	void setup() throws Exception {
		publicationService = mock(PublicationService.class);
		mogulService = mock(MogulService.class);
		managedFileService = mock(ManagedFileService.class);
		podcastService = mock(PodcastService.class);
	}

	@Test
    void feed() throws Exception {
        var mogulId = 1L;
        var podcastId = 1L;
        var mogul = new Mogul(mogulId, "jlong",
                "josh@joshlong.com", "clientId",
                "Josh", "Long" , new Date());
        var podcast = new Podcast(mogul.id(),
                podcastId, "the title", new Date(),
                List.of());
        var episodes = List.of(
                nextEpisode(mogulId, podcastId),
                nextEpisode(mogulId, podcastId),
                nextEpisode(mogulId, podcastId)
        );

        when(managedFileService.getPublicUrlForManagedFile(Mockito.anyLong()))
                .thenAnswer((Answer<String>) invocationOnMock -> {
                    var args = invocationOnMock.getArguments();
                    return "/public/managedfiles/" + args[0];
                });
        when(mogulService.getMogulById(mogulId)).thenReturn(mogul);
        when(podcastService.getPodcastById(podcastId)).thenReturn(podcast);
        when(podcastService.getPodcastEpisodesByPodcast(podcastId)).thenReturn(episodes);
        for (var episode : episodes)
            when(publicationService.getPublicationsByPublicationKeyAndClass(episode.id(),
                    Episode.class)).thenReturn(List.of(new Publication(mogulId, counter.incrementAndGet(),
                    "mock", new Date(), new Date(), Map.of(), "", Episode.class,
                    "https://bootifulpodcast.fm/episodes/" + episode.id(), Publication.State.PUBLISHED)));
		
    }

	private Episode nextEpisode(long mogulId, long podcastId) throws Exception {
		var nextId = counter.incrementAndGet();
		var producedAudio = nextManagedFile(mogulId, true, CommonMediaTypes.MP3.toString());
		var graphic = nextManagedFile(mogulId, true, MediaType.IMAGE_JPEG_VALUE);
		var producedGraphic = nextManagedFile(mogulId, true, MediaType.IMAGE_JPEG_VALUE);
		return new Episode(nextId, podcastId, "the title for episode " + nextId,
				"the description for episode " + nextId, new Date(), graphic, producedGraphic, producedAudio, true,
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