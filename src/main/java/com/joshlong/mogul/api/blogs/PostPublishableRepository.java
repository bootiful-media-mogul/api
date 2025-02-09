package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.PublishableRepository;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
class PostPublishableRepository implements PublishableRepository<Post> {

	private final BlogService blogService;

	PostPublishableRepository(BlogService blogService) {
		this.blogService = blogService;
	}

	@Override
	public boolean supports(Class<?> clazz) {
		return Post.class.isAssignableFrom(clazz);
	}

	@Override
	public Post find(Serializable serializable) {
		return this.blogService.getPostById((Long) serializable);
	}

}
