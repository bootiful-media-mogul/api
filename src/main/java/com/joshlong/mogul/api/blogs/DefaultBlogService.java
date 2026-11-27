package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.Paging;
import com.joshlong.mogul.api.SearchableDeletedEvent;
import com.joshlong.mogul.api.ai.AiClient;
import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.compositions.CompositionService;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import com.joshlong.mogul.utils.JdbcUtils;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Transactional
class DefaultBlogService implements BlogService {

	public final ApplicationEventPublisher publisher;

	private final RowMapper<Blog> blogRowMapper = new BlogRowMapper();

	private final CompositionService compositionService;

	// a post's owner is its blog's: blog_post has no mogul_id of its own. every query
	// that
	// feeds the PostRowMapper starts with this
	private static final String POST_SELECT = " select bp.*, b.mogul_id from blog_post bp join blog b on b.id = bp.blog_id ";

	private final RowMapper<Post> postRowMapper = new PostRowMapper();

	private final JdbcClient db;

	private final AiClient singularity;

	private final TransactionTemplate transactionTemplate;

	DefaultBlogService(JdbcClient db, AiClient singularity, ApplicationEventPublisher publisher,
			CompositionService compositionService, TransactionTemplate transactionTemplate) {
		this.db = db;
		this.singularity = singularity;
		this.publisher = publisher;
		this.compositionService = compositionService;
		this.transactionTemplate = transactionTemplate;
	}

	@ApplicationModuleListener
	void mogulCreated(MogulCreatedEvent createdEvent) {
		var mogul = createdEvent.mogul();
		if (this.getAllBlogsByMogul(mogul.id()).isEmpty()) {
			var podcast = this.createBlog(mogul.id(), mogul.givenName() + " " + mogul.familyName() + "'s Blog",
					mogul.givenName() + " " + mogul.familyName() + "' first Blog!");
			Assert.notNull(podcast,
					"there should be a newly created podcast associated with the mogul [" + mogul + "]");
		}
	}

	/**
	 * creates a {@link Post} in response to a {@link PostCreationRequestedEvent} (e.g.
	 * from the podcast-episode publisher plugin). runs synchronously inside its own
	 * transaction so that (a) {@link #createPost}'s own {@code PostCreatedEvent} fires on
	 * commit and (b) the created post is handed back to the caller via
	 * {@link PostCreationRequestedEvent#onCreated()} before {@code publishEvent} returns,
	 * letting the caller record the post's path as a publication outcome.
	 */
	@EventListener
	void onPostCreationRequested(PostCreationRequestedEvent e) {
		this.transactionTemplate.execute(status -> {
			var blog = this.getBlogById(e.blogId());
			Assert.notNull(blog, "the blog [" + e.blogId() + "] must exist");
			Assert.isTrue(blog.mogulId().equals(e.mogulId()),
					"the blog [" + e.blogId() + "] must belong to the requesting mogul [" + e.mogulId() + "]");
			var post = this.createPost(e.blogId(), new Date(), e.title(), e.content(), e.summary());
			e.onCreated().accept(post);
			return post;
		});
	}

	@Override
	public Collection<Blog> getAllBlogsByMogul(Long mogulId) {
		return this.db.sql("select * from blog b where b.mogul_id = ?")
			.params(mogulId)
			.query(this.blogRowMapper)
			.list();
	}

	@Override
	public Collection<Blog> getBlogsFor(long mogulId) {
		return this.db //
			.sql("select * from blog where mogul_id = ? ") //
			.params(mogulId) //
			.query(this.blogRowMapper) //
			.list();
	}

	@Override
	public Collection<Post> getPostsForBlog(long blogId) {
		return this.postsForBlog(blogId, false);
	}

	@Override
	public Collection<Post> getVisiblePostsForBlog(long blogId) {
		return this.postsForBlog(blogId, true);
	}

	@Override
	public Collection<Post> getNewestPostsForBlog(long blogId, Paging paging) {
		var params = new ArrayList<Object>();
		params.add(blogId);
		params.addAll(paging.params());
		var sql = POST_SELECT + " where bp.blog_id = ? order by bp.created desc, bp.id desc " + paging.sql();
		return this.db //
			.sql(sql) //
			.params(params) //
			.query(this.postRowMapper) //
			.list();
	}

	private Collection<Post> postsForBlog(long blogId, boolean visibleOnly) {
		var sql = POST_SELECT + " where bp.blog_id = ? " + (visibleOnly ? " and bp.visible = true " : "")
				+ " order by bp.created desc ";
		return this.db //
			.sql(sql) //
			.params(blogId) //
			.query(this.postRowMapper) //
			.list();
	}

	@Override
	public void setPostVisibility(Long postId, boolean visible) {
		this.db.sql("update blog_post set visible = ? where id = ?").params(visible, postId).update();
		this.publisher.publishEvent(new PostUpdatedEvent(this.getPostById(postId)));
	}

	@Override
	public Blog createBlog(Long mogulId, String title, String description) {
		var generatedKeyHolder = new GeneratedKeyHolder();
		this.db //
			.sql("""
						insert into blog(mogul_id , title , description)
						values (?,?,?)
						on conflict on constraint blog_mogul_id_title_key do update
						set title = excluded.title, description = excluded.description
					""")//
			.params(mogulId, title, description) //
			.update(generatedKeyHolder);
		var id = JdbcUtils.getIdFromKeyHolder(generatedKeyHolder);
		var blog = this.getBlogById(id.longValue());
		this.publisher.publishEvent(new BlogCreatedEvent(blog));

		return blog;
	}

	@Override
	public Blog updateBlog(Long mogulId, Long blogId, String title, String description, String rssUrl) {
		this.db.sql("update blog set title = ?, description = ?, rss_url = ? where id = ? and mogul_id = ? ") //
			.params(title, description, StringUtils.hasText(rssUrl) ? rssUrl : null, blogId, mogulId)//
			.update();
		var blog = this.getBlogById(blogId);
		this.publisher.publishEvent(new BlogUpdatedEvent(blog));
		return blog;
	}

	@Override
	public Blog getBlogById(Long id) {
		return this.db //
			.sql("select * from blog where id =? ") //
			.params(id) //
			.query(this.blogRowMapper) //
			.single();
	}

	@Override
	public Post getPostById(Long id) {
		var map = this.getPostsByIds(List.of(id));
		if (map.size() != 1)
			throw new IllegalStateException("there should be only one result");
		return map.get(map.keySet().iterator().next());
	}

	@Override
	public Map<Long, Post> getPostsByIds(Collection<Long> ids) {
		var map = new HashMap<Long, Post>();
		var list = this.db //
			.sql(POST_SELECT + " where bp.id = any(?) ") //
			.params(new SqlArrayValue("bigint", ids.toArray())) //
			.query(this.postRowMapper) //
			.list();
		for (var a : list) {
			map.put(a.id(), a);
		}
		return map;

	}

	@Override
	public void deletePost(Long postId) {
		var composition = this.getDescriptionComposition(postId);
		this.compositionService.deleteCompositionById(composition.id());
		this.db.sql("delete from blog_post where id = ? ").params(postId).update();
		this.publisher.publishEvent(new SearchableDeletedEvent(Post.class, postId));
	}

	@Override
	public void deleteBlog(Long id) {
		var blog = this.getBlogById(id);
		if (blog != null) {
			// todo get all the posts
			// get all the compositions
			// get all the managed_files on the posts (none yet but maybe one day)

			var posts = this.getPostsForBlog(id);
			for (var post : posts) {
				var postId = post.id();
				this.deletePost(postId);
			}

			this.db.sql("delete from blog where id = ? ").params(id).update();
		}
	}

	@Override
	public String summarize(String content) {
		var prompt = """
				Please summarize the text following the line. Reply with only the summary, and no other text:
				================================================================================================

				%s

				""";
		return this.singularity //
			.chat(prompt.formatted(content)) //
			.trim();
	}

	@Override
	public Post updatePost(Long postId, Date published, String title, String content, String summary, String rssSlug) {

		if (!StringUtils.hasText(summary))
			summary = "";

		this.db.sql("update  blog_post set title = ? , content = ?, summary = ?, rss_slug = ? where id = ?")//
			.params(title, content, summary, StringUtils.hasText(rssSlug) ? rssSlug : null, postId)//
			.update();
		if (null != published)
			this.db.sql("update blog_post set created= ? where id = ? ").params(published, postId).update();

		var postById = this.getPostById(postId);
		this.publisher.publishEvent(new PostUpdatedEvent(postById));
		return postById;
	}

	private Composition getDescriptionComposition(Long postId) {
		return this.compositionFor(postId, "description");
	}

	private Composition compositionFor(Long postId, String field) {
		// a composition is identified by the class and the key alone, so there's nothing
		// to be gained from loading the post just to read its id back off it.
		return this.compositionService.compose(Post.class, postId, field);
	}

	@Override
	public Post createPost(Long blogId, Date published, String title, String content, String summary) {
		return this.createPost(blogId, published, title, content, summary, null);
	}

	@Override
	public Post createPost(Long blogId, Date published, String title, String content, String summary, String rssSlug) {

		if (published == null)
			published = new Date();

		if (!StringUtils.hasText(summary))
			summary = "";

		var gkh = new GeneratedKeyHolder();
		this.db
			.sql(" insert into blog_post(blog_id, title, content ,summary, created, rss_slug ) values (?,?,?,?,?,?) ")
			.params(blogId, title, content, summary, published, StringUtils.hasText(rssSlug) ? rssSlug : null)
			.update(gkh);
		var id = JdbcUtils.getIdFromKeyHolder(gkh).longValue();
		var post = this.getPostById(id);
		var descriptionComposition = this.getDescriptionComposition(id);
		Assert.notNull(descriptionComposition, "description is null");
		this.publisher.publishEvent(new PostCreatedEvent(post));
		return post;
	}

	@Override
	public Collection<Post> findPostsByTitle(Long blogId, String title) {
		return this.db //
			.sql(POST_SELECT + " where bp.blog_id = ? and bp.title ilike ? ")//
			.params(blogId, title)//
			.query(this.postRowMapper)//
			.list();
	}

	@Override
	public Map<String, ManagedFile> resolveAssetsForPost(Long postId) {
		return Map.of();
	}

	@ApplicationModuleListener
	void onPostUpdatedEvent(PostUpdatedEvent postUpdatedEvent) {

	}

	@Override
	public Composition getBlogPostDescriptionComposition(Long postId) {
		return this.getDescriptionComposition(postId);
	}

	private static class PostRowMapper implements RowMapper<Post> {

		@Override
		public Post mapRow(ResultSet rs, int rowNum) throws SQLException {
			// getTimestamp, not getDate: getDate returns a java.sql.Date, which is a
			// calendar day with the time truncated to midnight. the column is a
			// timestamp, and the editor lets you set the time of day, so reading it
			// back through getDate threw away what had just been saved. every other row
			// mapper here -- Blog, Episode, Podcast -- already uses getTimestamp.
			return new Post(rs.getLong("mogul_id"), rs.getLong("id"), rs.getString("title"), rs.getTimestamp("created"),
					rs.getString("content"), rs.getBoolean("complete"), rs.getBoolean("visible"), new HashMap<>(),
					rs.getString("summary"), rs.getString("rss_slug"), rs.getLong("blog_id"));
		}

	}

	private static class BlogRowMapper implements RowMapper<Blog> {

		@Override
		public Blog mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Blog(rs.getLong("mogul_id"), rs.getLong("id"), rs.getString("title"),
					rs.getString("description"), rs.getString("rss_url"), rs.getTimestamp("created"));
		}

	}

}
