package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.compositions.ComposableOwnerResolver;
import org.springframework.stereotype.Component;

@Component
class PostComposableOwnerResolver implements ComposableOwnerResolver {

	private final BlogService blogService;

	PostComposableOwnerResolver(BlogService blogService) {
		this.blogService = blogService;
	}

	@Override
	public boolean supports(Class<?> composableClass) {
		return Post.class.equals(composableClass);
	}

	@Override
	public Long mogulId(Class<?> composableClass, Long compositionKey) {
		var post = this.blogService.getPostById(compositionKey);
		return post == null ? null : post.mogulId();
	}

}
