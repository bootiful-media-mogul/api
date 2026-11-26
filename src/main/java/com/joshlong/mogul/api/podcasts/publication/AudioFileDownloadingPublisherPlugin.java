package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.utils.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * this plugin doesn't really 'publish' anything. it just lets the user download the
 * produced final audio file. nonetheless, it's useful to have the server-side validation
 * of the state of the episode here.
 */
@Component(value = AudioFileDownloadingPublisherPlugin.PLUGIN_NAME)
class AudioFileDownloadingPublisherPlugin implements PodcastEpisodePublisherPlugin {

	static final String PLUGIN_NAME = "audioFile";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ManagedFileService managedFileService;

	AudioFileDownloadingPublisherPlugin(ManagedFileService managedFileService) {
		this.managedFileService = managedFileService;
	}

	@Override
	public String name() {
		return PLUGIN_NAME;
	}

	// a download is for right now: the user is waiting on the file
	@Override
	public boolean schedulable() {
		return false;
	}

	@Override
	public boolean canPublish(PublishContext<Episode> context) {
		return PodcastEpisodePublisherPlugin.super.canPublish(context) && context.payload().complete();
	}

	@Override
	public void publish(PublishContext<Episode> payload) {
		this.log.debug("downloading the produced audio file for episode # {}", payload.payload().id());
		var managedFileId = payload.payload().producedAudio().id();
		this.managedFileService.setManagedFileVisibility(managedFileId, true);
		// the re-render has already happened by the time we get here, so this URL carries
		// the etag of the audio that was just produced -- not of whatever the previous
		// publication left at the same key. that is the whole value of recording it as
		// the outcome: it is the one URL that cannot be a generation behind.
		var url = this.managedFileService.getDownloadableUrlForManagedFile(managedFileId);
		Assert.hasText(url,
				"there is no downloadable URL for the produced audio of episode #" + payload.payload().id());
		payload.success(this.name(), UriUtils.uri(url));
	}

	@Override
	public boolean unpublish(UnpublishContext<Episode> context) {
		this.log.debug("can't 'unpublish' a downloaded file for publication # {}", context.publication().id());
		return true;
	}

}
