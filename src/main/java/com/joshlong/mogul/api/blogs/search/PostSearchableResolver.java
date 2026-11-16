package com.joshlong.mogul.api.blogs.search;

import com.joshlong.mogul.api.AbstractSearchableResolver;
import com.joshlong.mogul.api.SearchableResult;
import com.joshlong.mogul.api.blogs.BlogService;
import com.joshlong.mogul.api.blogs.Post;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Component
class PostSearchableResolver extends AbstractSearchableResolver<Post> {

	private final BlogService blogService;

	PostSearchableResolver(BlogService blogService) {
		super(Post.class);
		this.blogService = blogService;
	}

	@Override
	public List<SearchableResult<Post>> results(List<Long> searchableIds) {
		if (searchableIds == null || searchableIds.isEmpty())
			return Collections.emptyList();
		var postsMap = this.blogService.getPostsByIds(searchableIds);
		if (postsMap.isEmpty())
			return Collections.emptyList();
		var searchableResults = new ArrayList<SearchableResult<Post>>();
		Assert.state(!postsMap.isEmpty(), "Posts must not be empty");
		for (var post : postsMap.values()) {
			searchableResults.add(this.buildResultFor(post, post.content()));
		}
		return searchableResults;
	}

	private SearchableResult<Post> buildResultFor(Post post, String transcript) {
		return new SearchableResult<>(post.searchableId(), post, post.title(), transcript, post.id(), post.created(),
				this.type, post.mogulId());
	}

	@Override
	public Collection<Long> searchableIds(Long mogulId) {
		var ids = new ArrayList<Long>();
		for (var blog : this.blogService.getBlogsFor(mogulId))
			for (var post : this.blogService.getPostsForBlog(blog.id()))
				ids.add(post.id());
		return ids;
	}

	@Override
	public Post find(Long key) {
		return this.blogService.getPostById(key);
	}

}
