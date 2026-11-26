package com.joshlong.mogul.api.blogs.publication;

import com.joshlong.mogul.api.blogs.BlogService;
import com.joshlong.mogul.api.blogs.Post;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.publications.PublicationService;
import com.joshlong.mogul.utils.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static com.joshlong.mogul.api.blogs.publication.MarkdownFileBlogPostPublisherPlugin.PLUGIN_NAME;

@Component(value = PLUGIN_NAME)
class MarkdownFileBlogPostPublisherPlugin implements BlogPostPublisherPlugin {

	static final String PLUGIN_NAME = "blogPostMarkdownFile";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final BlogService blogService;

	private final PostPreviewsService postPreviewsService;

	private final ManagedFileService managedFileService;

	MarkdownFileBlogPostPublisherPlugin(BlogService blogService, PostPreviewsService postPreviewsService,
			ManagedFileService managedFileService) {
		this.blogService = blogService;
		this.postPreviewsService = postPreviewsService;
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
	public void publish(PublishContext<Post> publishContext) {
		var postId = publishContext.payload().id();
		var publicationId = Long.parseLong(publishContext.context().get(PublicationService.PUBLICATION_ID));
		var mogulId = Long.parseLong(publishContext.context().get(PublicationService.MOGUL_ID));
		for (var entry : publishContext.context().entrySet()) {
			this.log.info("{}={}", entry.getKey(), entry.getValue());
		}
		this.log.info("publishing Markdown file {} for post", publishContext.payload().id());
		var postById = this.blogService.getPostById(postId);
		var byteArrayResource = new ByteArrayResource(postById.content().getBytes());
		var preview = this.postPreviewsService.createPostPreview(mogulId, publicationId, postId);
		this.managedFileService.write(preview.managedFile().id(),
				"blog-post-" + postId + "preview-" + UUID.randomUUID() + ".md",
				MediaType.parseMediaType("text/markdown"), byteArrayResource);
		this.managedFileService.setManagedFileVisibility(preview.managedFile().id(), true);

		// todo find some way to pass to this place or in the context the servlet context
		// so we can get the host:port
		// i can't use the private url for this managedFile because it's relative and not
		// absolute. i need the host:port

		publishContext.success(PLUGIN_NAME,
				UriUtils.uri(this.managedFileService.getPublicUrlForManagedFile(preview.managedFile().id())));
	}

	@Override
	public boolean unpublish(UnpublishContext<Post> uc) {
		return false;
	}

}
