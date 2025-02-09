package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.blogs.Blog;
import com.joshlong.mogul.api.blogs.BlogService;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.Episode;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Set;

/**
 * this will initialize a new {@link com.joshlong.mogul.api.blogs.Post blog post} with the
 * contents of the transcript of a given {@link Episode podcast episode}. we can source
 * things like tags, titles, descriptions, etc., from the podcast, too.
 * <p>
 * this is the first plugin to require that the user provide some data upfront before it
 * can proceed. Namely: in which blog should the post be made?
 */

// todo where should this class live? what if one day we decide to have the ability to
// publish a podcast from a blog? will that create a tangle?
@Component(PodcastEpisodeBlogPostPubliserPlugin.PLUGIN_NAME)
class PodcastEpisodeBlogPostPubliserPlugin implements PodcastEpisodePublisherPlugin {

	public static final String PLUGIN_NAME = "podcast-blog";

	/**
	 * the user needs to provide this in the event of publication.
	 */
	public static final String CONTEXT_BLOG_ID = "blogId";

	private final BlogService blogService;

	private final MogulService mogulService;

	PodcastEpisodeBlogPostPubliserPlugin(BlogService blogService, MogulService mogulService) {
		this.blogService = blogService;
		this.mogulService = mogulService;
	}

	@Override
	public String name() {
		return PLUGIN_NAME;
	}

	@Override
	public Set<String> getRequiredSettingKeys() {
		return Set.of(CONTEXT_BLOG_ID);
	}

	private boolean confirmMogulHasRightsToBlog(Blog targetBlog) {
		// todo can this run in the same thread? can we use getCurrentMogul()?
		// Assert.state( blogById.mogulId() .equals( ));
		return true;
	}

	@Override
	public void publish(Map<String, String> context, Episode payload) {

		var blogId = Long.parseLong(context.get(CONTEXT_BLOG_ID));
		var targetBlog = blogService.getBlogById(blogId);
		Assert.notNull(targetBlog, "the blog must exist");
		Assert.state(this.confirmMogulHasRightsToBlog(targetBlog),
				"the mogul does not have the right " + "to publish content to this blog");

		// todo
		// get transcript of all segments
		// concatenate them
		// get title , description

		var title = payload.title();
		var description = payload.description();

	}

	@Override
	public boolean unpublish(Map<String, String> context, Publication publication) {
		return false;
	}

}
