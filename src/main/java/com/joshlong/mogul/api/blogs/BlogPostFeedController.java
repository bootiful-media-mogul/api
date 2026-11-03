package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.feeds.Entry;
import com.joshlong.mogul.api.feeds.EntryMapper;
import com.joshlong.mogul.api.feeds.Feeds;
import com.joshlong.mogul.api.mogul.MogulService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Controller
@ResponseBody
class BlogPostFeedController {

	private static final String BLOG_FEED_URL = "/public/feeds/moguls/{mogulId}/blogs/{blogId}/posts.atom";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Feeds feeds;

	private final MogulService mogulService;

	private final BlogService blogService;

	BlogPostFeedController(Feeds feeds, MogulService mogulService, BlogService blogService) {
		this.feeds = feeds;
		this.mogulService = mogulService;
		this.blogService = blogService;
	}

	private static UUID longToUuid(long id) {
		return new UUID(0, id);
	}

	private static String computedPath(Post post) {
		return "/blogs/%s/posts/%s".formatted(post.blogId(), post.id());
	}

	private static Optional<URI> validRoot(Blog blog) {
		if (!StringUtils.hasText(blog.rssUrl()))
			return Optional.empty();
		try {
			var root = URI.create(blog.rssUrl());
			if (!root.isAbsolute() || root.getHost() == null)
				return Optional.empty();
			return Optional.of(root);
		}
		catch (IllegalArgumentException _) {
			return Optional.empty();
		}
	}

	private static String rootedUrl(URI root, String path) {
		var rootString = root.toString().replaceFirst("/+$", "");
		var pathString = path.replaceFirst("^/+", "");
		return rootString + "/" + pathString;
	}

	private static String urlFor(Blog blog, Post post, String existingUrl) {
		var fallback = StringUtils.hasText(existingUrl) ? existingUrl : computedPath(post);
		var root = validRoot(blog);
		if (root.isEmpty())
			return fallback;
		if (StringUtils.hasText(post.rssSlug()))
			return rootedUrl(root.get(), post.rssSlug());
		return rootedUrl(root.get(), computedPath(post));
	}

	// the feed is public, and sites like bootifulpodcast.fm read it from the browser
	@CrossOrigin
	@GetMapping(value = BLOG_FEED_URL)
	String feed(@PathVariable long mogulId, @PathVariable long blogId)
			throws IOException, ParserConfigurationException, TransformerException {
		this.log.debug("producing the RSS feed for " + BLOG_FEED_URL + " for mogulId {} and blogId {}", mogulId,
				blogId);
		var mogul = this.mogulService.getMogulById(mogulId);
		var blog = this.blogService.getBlogById(blogId);
		Assert.state(blog.mogulId() == mogulId, "the blog's mogulId does not match the mogulId in the path");
		var posts = this.blogService.getVisiblePostsForBlog(blogId);
		var blogPostRowMapper = new BlogPostEntryMapper(blog, Map.of());
		return this.feeds.createMogulAtomFeed(blog.title(), BLOG_FEED_URL, blog.created().toInstant(),
				mogul.givenName() + " " + mogul.familyName(), longToUuid(blogId).toString(), posts, blogPostRowMapper);
	}

	private record BlogPostEntryMapper(Blog blog, Map<Long, String> urls) implements EntryMapper<Post> {

		@Override
		public Entry map(Post post) {
			var img = (Entry.Image) null;
			var uuid = longToUuid(post.id());
			var summary = post.summary();
			var url = urlFor(this.blog, post, this.urls.get(post.id()));
			return new Entry(uuid.toString(), new Date(post.created().getTime()).toInstant(), post.title(), url,
					summary, Map.of("id", Long.toString(post.id())), img);
		}

	}

}
