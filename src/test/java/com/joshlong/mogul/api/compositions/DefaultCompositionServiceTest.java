package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import static org.junit.jupiter.api.Assertions.*;

@Transactional
@SpringBootTest
class DefaultCompositionServiceTest {

	private final CompositionService compositionService;

	private final JdbcClient db;

	private final ManagedFileService managedFileService;

	DefaultCompositionServiceTest(@Autowired CompositionService compositionService, @Autowired JdbcClient db,
			@Autowired ManagedFileService managedFileService) {
		this.compositionService = compositionService;
		this.managedFileService = managedFileService;
		this.db = db;
	}

	@BeforeAll
	static void reset(@Autowired JdbcClient db) {
		db.sql("delete from composition_attachment ").update();
		db.sql("delete from composition").update();
	}

	@Test
	void composeAndAttach() {
		var mogulId = this.db.sql(" select id from mogul limit 1 ").query(Long.class).single();
		var field = "description";
		var compositionKey = "podcast-episode-" + 3;
		var compositionForDescription = this.compositionService.compose(mogulId, compositionKey, field);
		Assertions.assertTrue(compositionForDescription.attachments().isEmpty(),
				"there should be no attachments for this composition");
		assertTrue(StringUtils.hasText(compositionForDescription.key()));
		assertEquals(field, compositionForDescription.field());
		assertNotNull(compositionForDescription.id());
		assertEquals(compositionKey, compositionForDescription.key());
		var managedFile = this.managedFileService.createManagedFile(mogulId, "foo", "bar", "simple.png", 0,
				MediaType.IMAGE_JPEG, true);
		var key = "picture of author with fish";
		var pictureOfAuthorWithFish = this.compositionService.attach(compositionForDescription.id(), key, managedFile);
		assertNotNull(pictureOfAuthorWithFish, "the picture of author with fish should not be null");
		var composition = this.compositionService.compose(mogulId, compositionKey, field);
		assertEquals(1, composition.attachments().size(), "there should be one attachment for this composition");
		assertEquals(key, composition.attachments().iterator().next().key());

		var managedFile2 = this.managedFileService.createManagedFile(mogulId, "foo", "bar", "simple2.png", 0,
				MediaType.IMAGE_JPEG, true);
		var key2 = "picture of author with fish, pt 2";
		var pictureOfAuthorWithFish2 = this.compositionService.attach(compositionForDescription.id(), key2,
				managedFile2);
		assertEquals(key2, pictureOfAuthorWithFish2.key());
		composition = this.compositionService.compose(mogulId, compositionKey, field);
		assertEquals(2, composition.attachments().size(), "there should be two attachments for this composition");

	}

}