package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.ai.AiClient;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.utils.JdbcUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
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

	private final RowMapper<Blog> blogRowMapper = new BlogRowMapper();

	private final RowMapper<Post> postRowMapper = new PostRowMapper();

	private final JdbcClient db;

	private final AiClient singularity;

	public final ApplicationEventPublisher publisher;

	DefaultBlogService(JdbcClient db, AiClient singularity, ApplicationEventPublisher publisher) {
		this.db = db;
		this.singularity = singularity;
		this.publisher = publisher;
	}

	@Override
	public Blog createBlog(Long mogulId, String title, String description) {
		return this.createOrUpdateBlog(mogulId, title, description);
	}

	@Override
	public Blog updateBlog(Long mogulId, String title, String description) {
		return this.createOrUpdateBlog(mogulId, title, description);
	}

	private Blog createOrUpdateBlog(Long mogulId, String title, String description) {
		var generatedKeyHolder = new GeneratedKeyHolder();
		this.db.sql("""
					insert into blog(mogul_id , title , description)
					values (?,?,?)
					on conflict on constraint blog_mogul_id_title_key do update
					set title = excluded.title, description = excluded.description
				""").params(mogulId, title, description).update(generatedKeyHolder);
		var id = JdbcUtils.getIdFromKeyHolder(generatedKeyHolder);
		return this.getBlogById(id.longValue());

	}

	@Override
	public Blog getBlogById(Long id) {
		return this.db.sql("select * from blog where id =? ").params(id).query(this.blogRowMapper).single();
	}

	@Override
	public Post getPostById(Long id) {
		return this.db.sql(" select * from blog_post where id =  ? ").params(id).query(this.postRowMapper).single();
	}

	@Override
	public void deleteBlog(Long id) {
		if (this.getBlogById(id) != null) {

			// todo queue up all the ManagedFiles associated with this blog for deletion
			this.db.sql("delete from blog_post where  blog_id =?").params(id).update();

			this.db.sql("delete from blog where id =?").params(id).update();
		}
	}

	@Override
	public String summarize(String content) {
		var prompt = """
				Please summarize the text following the line. Reply with only the summary, and no other text:
				================================================================================================

				%s

				""";
		return this.singularity.chat(prompt.formatted(content)).trim();
	}

	@Override
	public Asset createPostAsset(Long postId, String key, ManagedFile managedFile) {
		return null;
	}

	private static class PostRowMapper implements RowMapper<Post> {

		@Override
		public Post mapRow(ResultSet rs, int rowNum) throws SQLException {
			var arr = rs.getArray("tags");
			System.out.println("------------------------");
			System.out.println(arr);
			System.out.println(arr.getClass().getName());
			System.out.println("------------------------");
			var tags = new String[0];
			if (arr.getArray() instanceof String[] tagsStringArray) {
				tags = tagsStringArray;
			}
			return new Post(rs.getLong("blog_id"), rs.getLong("id"), rs.getString("title"), rs.getDate("created"),
					rs.getString("content"), tags, rs.getBoolean("complete"), new HashMap<>());
		}

	}

	@Override
	public Post updatePost(Long postId, String title, String content, String[] tags, Set<Asset> assets) {
		return null;
	}

	@Override
	public Post createPost(Long blogId, String title, String content, String[] tags, Set<Asset> assets) {
		var gkh = new GeneratedKeyHolder();
		this.db.sql("""
				 insert into blog_post(blog_id, title, content ,    tags) values  (?,?,?,?)
				""").params(blogId, title, content, tags).update(gkh);
		var id = JdbcUtils.getIdFromKeyHolder(gkh);
		return getPostById(id.longValue());
	}

	@Override
	public Map<String, ManagedFile> resolveAssetsForPost(Long postId) {
		return Map.of();
	}

	private static class BlogRowMapper implements RowMapper<Blog> {

		@Override
		public Blog mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Blog(rs.getLong("mogul_id"), rs.getLong("id"), rs.getString("title"),
					rs.getString("description"), rs.getDate("created"), new HashSet<>());
		}

	}

	@EventListener
	void onPostUpdatedEvent(PostUpdatedEvent postUpdatedEvent) {
		// todo update the markdown => html
		// todo update the summary
		// todo should we store timestamps so we can see when the post was updated and
		// when the markdown/summary were last regenerated?

		// todo we should check to see if the text for the title / description themselves
		// were actually updated before publishing this event.
	}

}

record PostUpdatedEvent(int postId, String title, String description) {
}
