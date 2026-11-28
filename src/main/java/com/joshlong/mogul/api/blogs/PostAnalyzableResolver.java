package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.AbstractAnalyzableResolver;
import com.joshlong.mogul.api.AnalyzableSection;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

/** a post is one section: the whole of its content */
@Component
class PostAnalyzableResolver extends AbstractAnalyzableResolver<Post> {

	private final BlogService blogService;

	PostAnalyzableResolver(BlogService blogService) {
		super(Post.class);
		this.blogService = blogService;
	}

	@Override
	public Post find(Long key) {
		return this.blogService.getPostById(key);
	}

	@Override
	public Long mogulId(Post analyzable) {
		return analyzable.mogulId();
	}

	@Override
	public List<AnalyzableSection> sections(Post analyzable) {
		return StringUtils.hasText(analyzable.content())
				? List.of(new AnalyzableSection(Long.toString(analyzable.id()), null, analyzable.content()))
				: List.of();
	}

}
