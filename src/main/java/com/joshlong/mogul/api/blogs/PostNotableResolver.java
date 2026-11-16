package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.AbstractNotableResolver;
import org.springframework.stereotype.Component;

@Component
class PostNotableResolver extends AbstractNotableResolver<Post> {

	private final BlogService blogService;

	PostNotableResolver(BlogService blogService) {
		super(Post.class);
		this.blogService = blogService;
	}

	@Override
	public Post find(Long key) {
		return this.blogService.getPostById(key);
	}

	@Override
	public Long mogulId(Post notable) {
		return notable.mogulId();
	}

}
