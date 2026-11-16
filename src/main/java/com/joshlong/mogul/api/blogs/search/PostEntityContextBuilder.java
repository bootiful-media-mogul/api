package com.joshlong.mogul.api.blogs.search;

import com.joshlong.mogul.api.EntityContext;
import com.joshlong.mogul.api.EntityContextBuilder;
import com.joshlong.mogul.api.blogs.BlogService;
import com.joshlong.mogul.api.blogs.Post;
import com.joshlong.mogul.utils.TypeUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

@Component
class PostEntityContextBuilder implements EntityContextBuilder<Post> {

	private final BlogService blogService;

	PostEntityContextBuilder(BlogService blogService) {
		this.blogService = blogService;
	}

	@Override
	public EntityContext buildContextFor(Long mogulId, Long entityId) {
		var post = this.blogService.getPostById(entityId);
		Assert.state(post != null && post.mogulId().equals(mogulId),
				() -> "there is no post #" + entityId + " for mogul #" + mogulId);
		return new EntityContext(TypeUtils.typeName(Post.class), Map.of("postId", post.id(), "blogId", post.blogId()));
	}

}
