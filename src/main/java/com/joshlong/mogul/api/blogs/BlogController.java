package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

import java.time.OffsetDateTime;
import java.util.Collection;

@Controller
class BlogController {

	private final BlogService service;

	private final MogulService mogulService;

	private final Logger log = LoggerFactory.getLogger(BlogController.class);

	BlogController(BlogService service, MogulService mogulService) {
		this.service = service;
		this.mogulService = mogulService;
	}

	@MutationMapping
	Post createPost(@Argument Long blogId, @Argument String title, @Argument String content, @Argument String summary,
			@Argument String rssSlug) {
		this.ownedBlog(blogId);
		return this.service.createPost(blogId, null, title, content, summary, rssSlug);
	}

	@MutationMapping
	boolean deleteBlog(@Argument Long blogId) {
		this.ownedBlog(blogId);
		this.service.deleteBlog(blogId);
		return true;
	}

	@MutationMapping
	boolean deletePost(@Argument Long postId) {
		// outside the try: a refusal must not be swallowed and reported as a success
		this.ownedPost(postId);
		try {
			this.service.deletePost(postId);
		} //
		catch (Throwable throwable) {
			this.log.error("Error while deleting post", throwable);
		}
		return true;
	}

	@MutationMapping
	boolean setBlogPostVisibility(@Argument Long postId, @Argument Boolean visible) {
		this.ownedPost(postId);
		this.service.setPostVisibility(postId, Boolean.TRUE.equals(visible));
		return true;
	}

	@SchemaMapping
	Composition descriptionComposition(Post post) {
		return this.service.getBlogPostDescriptionComposition(post.id());
	}

	@QueryMapping
	Collection<Post> blogPostsByBlog(@Argument Long blogId, @Argument String query, @Argument Integer limit) {
		this.ownedBlog(blogId);
		return this.service.findPostsForBlog(blogId, query, limit);
	}

	@MutationMapping
	boolean updatePost(@Argument Long postId, @Argument String title, @Argument String description,
			@Argument String summary, @Argument String rssSlug, @Argument OffsetDateTime created) {
		this.ownedPost(postId);
		// updatePost's second parameter is written straight to blog_post.created; a null
		// leaves the existing value alone.
		this.service.updatePost(postId, DateUtils.toDate(created), title, description, summary, rssSlug);
		return true;
	}

	@MutationMapping
	String summarize(@Argument String content) {
		return this.service.summarize(content);
	}

	@QueryMapping
	Post postById(@Argument Long postId) {
		return this.ownedPost(postId);
	}

	@SchemaMapping
	OffsetDateTime created(Post blog) {
		return DateUtils.forDate(blog.created());
	}

	@ApplicationModuleListener
	void blogCreatedEventNotifyingListener(BlogCreatedEvent event) {
		var ne = NotificationEvent.visibleNotificationEventFor(event.blog().mogulId(), event,
				Long.toString(event.blog().id()), event.blog().title());
		NotificationEvents.notify(ne);
	}

	@ApplicationModuleListener
	void blogDeletedEventNotifyingListener(BlogDeletedEvent event) {
		var ne = NotificationEvent.visibleNotificationEventFor(event.blog().mogulId(), event,
				Long.toString(event.blog().id()), event.blog().title());
		NotificationEvents.notify(ne);
	}

	@ApplicationModuleListener
	void blogUpdatedEventNotifyingListener(BlogUpdatedEvent podcastUpdatedEvent) {
		var ne = NotificationEvent.visibleNotificationEventFor(podcastUpdatedEvent.blog().mogulId(),
				podcastUpdatedEvent, Long.toString(podcastUpdatedEvent.blog().id()),
				podcastUpdatedEvent.blog().title());
		NotificationEvents.notify(ne);
	}

	@MutationMapping
	boolean updateBlog(@Argument Long blogId, @Argument String title, @Argument String description,
			@Argument String rssUrl) {
		this.service.updateBlog(this.mogulService.getCurrentMogul().id(), blogId, title, description, rssUrl);
		return true;
	}

	@MutationMapping
	Blog createBlog(@Argument String title, @Argument String description) {
		var mogul = this.mogulService.getCurrentMogul();
		return this.service.createBlog(mogul.id(), title, description);
	}

	@QueryMapping
	Blog blogById(@Argument Long blogId) {
		return this.ownedBlog(blogId);
	}

	@SchemaMapping
	OffsetDateTime created(Blog blog) {
		return DateUtils.forDate(blog.created());
	}

	// every operation that takes an id checks that it's the signed-in mogul's, before
	// doing anything: ids are sequential, so they're easy to guess

	private Blog ownedBlog(Long blogId) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		var blog = blogId == null ? null : this.service.getBlogById(blogId);
		Assert.state(blog != null && mogulId.equals(blog.mogulId()),
				() -> "there is no blog #" + blogId + " for mogul #" + mogulId);
		return blog;
	}

	private Post ownedPost(Long postId) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		var post = postId == null ? null : this.service.getPostById(postId);
		Assert.state(post != null && mogulId.equals(post.mogulId()),
				() -> "there is no post #" + postId + " for mogul #" + mogulId);
		return post;
	}

	@QueryMapping
	Collection<Blog> blogs() {
		var mogul = this.mogulService.getCurrentMogul();
		return this.service.getBlogsFor(mogul.id());
	}

}
