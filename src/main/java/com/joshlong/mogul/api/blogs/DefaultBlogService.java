package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.utils.JdbcUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Set;

/*
TODO put these in github issues!
* create a blog
* create a post
* we need to support full-text search. Lucene? Do we have different Lucenes for each user?
        Can we do Elasticsearch? the one hosted on Elastic.co ? Does postgresql provide
        a full text search of any worth?  i think this should be a cross-cutting thing. podcasts, youtube, blogs, etc.,
        should all be able to benefit from searchability. should we add a new interface - `Searchable`, etc? Maybe it exports
        indexable text and metadata for each asset type that we could show in the search results, query by the metadata, etc.,
        to drive feeds eg, of all mogul's content, of a particular mogul's content, or a particular mogul's blogs/podcasts/etc?

* we need a mechanism by which to create tags and normalize them and so on so that we can
    support autocomplete for other blogs published by a given author
*  we need a way to attach multiple mogul's to a particular blog post so that it
        would show up in both mogul's feeds.
* we want to as friction-free as possible derive two things: the summary and the html
** html: lets say the user just types plain text. is it markdown? do we support straight html?
        do we want to allow a full WYSIWYG editor? I think we should normalize all text to Markdown, but that won't render.
        so we need to recompute the HTML mark for publication / display on the web.
** summary. we can compute  summary (eg, with the same mechanism in writing tools) for the post, or we can
        let the user provide their own


*/

@Service
@Transactional
class DefaultBlogService implements BlogService {

	private final JdbcClient db;

	public final ApplicationEventPublisher publisher;

	DefaultBlogService(JdbcClient db, ApplicationEventPublisher publisher) {
		this.db = db;
		this.publisher = publisher;
	}

	@Override
	public Blog createBlog(Long mogulId, String title) {

		var generatedKeyHolder = new GeneratedKeyHolder();
		this.db.sql(
				" insert into blog (mogul , title) values (?,?) on conflict on constraint podcast_mogul_id_title_key do update set title = excluded.title ")
			.params(mogulId, title)
			.update(generatedKeyHolder);
		var id = JdbcUtils.getIdFromKeyHolder(generatedKeyHolder);
		var blog = this.getBlogById(id.longValue());
		return blog;
	}

	@Override
	public Blog getBlogById(Long id) {
		return null;
	}

	@Override
	public void deleteBlog(Long id) {

	}

	@Override
	public String summarize(String content) {
		return "";
	}

	@Override
	public Asset createPostAsset(Long postId, String key, ManagedFile managedFile) {
		return null;
	}

	@Override
	public Post updatePost(Long postId, String title, String summary, String content, String[] tags,
			Set<Asset> assets) {
		return null;
	}

	@Override
	public Post createPost(Long postId, String title, String summary, String content, String[] tags,
			Set<Asset> assets) {
		return null;
	}

	@Override
	public Map<String, ManagedFile> resolveAssetsForPost(Long postId) {
		return Map.of();
	}

}
