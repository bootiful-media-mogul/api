package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public interface BlogService {

	Collection<Blog> getAllBlogsByMogul(Long mogulId);

	Collection<Blog> getBlogsFor(long mogulId);

	Collection<Post> getPostsForBlog(long blogId);

	Collection<Post> getVisiblePostsForBlog(long blogId);

	/**
	 * the blog's newest posts, at most {@code limit} of them, or all of them for a
	 * {@literal null} limit.
	 */
	Collection<Post> getNewestPostsForBlog(long blogId, @Nullable Integer limit);

	void deletePost(Long postId);

	void setPostVisibility(Long postId, boolean visible);

	Blog createBlog(Long mogulId, String title, String description);

	Blog updateBlog(Long mogulId, Long blogId, String title, String description, String rssUrl);

	Blog getBlogById(Long id);

	Post getPostById(Long id);

	Map<Long, Post> getPostsByIds(Collection<Long> ids);

	void deleteBlog(Long id);

	String summarize(String content);

	Post updatePost(Long postId, Date publishedAt, String title, String content, String summary, String rssSlug);

	Post createPost(Long blogId, Date published, String title, String content, String summary);

	Post createPost(Long blogId, Date published, String title, String content, String summary, String rssSlug);

	Collection<Post> findPostsByTitle(Long blogId, String title);

	/**
	 * I imagine a world whereas you type text and reference images that we resolve all
	 * URLs and download them, making them available in the ManagedFile file system as
	 * public ManagedFiles. You could also choose to add an image and then reference it by
	 * its id or a key or something. we'll discover it and automatically replace it with
	 * the actual source.
	 */
	Map<String, ManagedFile> resolveAssetsForPost(Long postId);

	Composition getBlogPostDescriptionComposition(Long postId);

}
