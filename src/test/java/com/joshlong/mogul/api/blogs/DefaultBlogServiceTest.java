package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.mogul.Mogul;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootTest
class DefaultBlogServiceTest {

	private final JdbcClient db;

	private final BlogService blogService;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final RowMapper<Mogul> MOGUL_ROW_MAPPER = (rs, rowNum) -> new Mogul(rs.getLong("id"),
			rs.getString("username"), rs.getString("email"), rs.getString("client_id"), rs.getString("given_name"),
			rs.getString("family_name"));

	private static final AtomicLong MOGUL = new AtomicLong(-1L);

	DefaultBlogServiceTest(@Autowired JdbcClient db, @Autowired BlogService blogService) {
		this.db = db;
		this.blogService = blogService;
		Assertions.assertNotNull(this.blogService, "the blogService should not be null");
		Assertions.assertNotNull(this.db, "the database should not be null");
	}

	@BeforeAll
	static void reset(@Autowired JdbcClient db) {
		MOGUL.set(db.sql("select * from mogul where email ilike ? ")
			.params("%" + "josh@joshlong.com" + "%")
			.query(MOGUL_ROW_MAPPER)
			.single()
			.id());
		Assertions.assertTrue(MOGUL.get() > 0, "you must specify a valid mogul ID");
	}

	@Test
	void createBlogAndPost() {

		var blog = this.blogService.createBlog(MOGUL.get(), "a wonderful blog for mogul " + MOGUL.get(),
				"this blog will be all about this that and the rest");

		Assertions.assertNotNull(blog, "the mogul should not be null");
		Assertions.assertNotNull(blog.created(), "the created data should not be null");
		Assertions.assertTrue(StringUtils.hasText(blog.title()), "the title should not be null");
		Assertions.assertTrue(StringUtils.hasText(blog.description()), "the description should not be null");
		Assertions.assertNotNull(blog.posts(), "there should be a non-null posts collection");

		Assertions.assertTrue(blog.equals(this.blogService.getBlogById(blog.id())));

		// now let's create a post

		var split = "spring,java,crm".split(",");
		var post = this.blogService.createPost(blog.id(), "this is a post for my new blog!  ",
				"this is some sample content that Im sure will be double dope indeed", split, new HashSet<>());
		Assertions.assertFalse(post.complete());
		Assertions.assertNotNull(post.created());
		Assertions.assertTrue(StringUtils.hasText(post.content()));
		Assertions.assertTrue(StringUtils.hasText(post.title()));
		Assertions.assertTrue(StringUtils.hasText(post.content()));
		var ctr = 0;
		for (var t : post.tags()) {
			for (var createdTag : split)
				if (createdTag.equals(t))
					ctr++;
		}
		Assertions.assertEquals(ctr, split.length, "there should be 3 matches for the tags");

	}

	@Test
	void summarize() {
		var content = """
				Per Wikipedia.org: Mario Party DS[a] is a 2007 party video game developed by Hudson Soft and published by Nintendo for the Nintendo DS. It is the second handheld game in the Mario Party series, as well as the last game in the series to be developed by Hudson Soft, as all subsequent titles have been developed by Nintendo Cube. The game was re-released on the Virtual Console for the Wii U in 2016.

				Like most installments in the Mario Party series, Mario Party DS features characters from the Mario franchise competing in a board game with a variety of minigames, many of which utilize the console's unique features. Up to four human players can compete at a time, though characters can also be computer-controlled. The game features a single-player story mode as well as several other game modes.

				Mario Party DS received mixed reviews, with general praise for its minigame variety and criticism for its absence of an online multiplayer mode. The game has sold more than nine million units worldwide, making it the 11th-best-selling game for the Nintendo DS. Mario Party DS was succeeded by Mario Party 9 for the Wii in 2012.
				""";
		var summary = this.blogService.summarize(content);
		this.log.info("summary: {}", summary);
		Assertions.assertTrue(content.length() > summary.length(), "the summary should be smaller than the content");
	}

	@Test
	void createPostAsset() {
	}

	@Test
	void resolveAssetsForPost() {
	}

}