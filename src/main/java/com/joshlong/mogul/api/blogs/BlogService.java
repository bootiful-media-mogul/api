package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Map;
import java.util.Set;

public interface BlogService {

	// blog
	Blog createBlog(Long mogulId, String title);

	Blog getBlogById(Long id);

	void deleteBlog(Long id);

	String summarize(String content);

	Asset createPostAsset(Long postId, String key, ManagedFile managedFile);

	Post updatePost(Long postId, String title, String summary, String content, String[] tags, Set<Asset> assets);

	// posts
	Post createPost(Long postId, String title, String summary, String content, String[] tags, Set<Asset> assets);

	/**
	 * I imagine a world whereas you type text and reference images that we resolve all
	 * URLs and download them, making them available in the ManagedFile file system as
	 * public ManagedFiles. You could also choose to add an image and then reference it by
	 * its id or a key or something. we'll discover it and automatically replace it with
	 * the actual source.
	 */
	Map<String, ManagedFile> resolveAssetsForPost(Long postId);

}
