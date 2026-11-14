package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.mogul.MogulStatus;
import com.joshlong.mogul.api.mogul.MogulStatusService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * the home page's calendar asks for every publication in a range, whatever it's about. a
 * publication sits on the calendar at the moment it went out, else the moment it's due,
 * else the moment it was asked for, and each one says what it was about.
 */
@SpringBootTest
class PublicationsBetweenTest {

	@Test
	void publicationsLandWhereTheyGoOut(@Autowired JdbcClient db, @Autowired PublicationService publicationService,
			@Autowired MogulStatusService mogulStatusService, @Autowired MogulService mogulService,
			@Autowired TextEncryptor textEncryptor) {
		var mogulId = db.sql("select id from mogul").query((rs, _) -> rs.getLong("id")).list().getFirst();
		// the calendar only ever shows the authenticated mogul's own publications
		var username = mogulService.getMogulById(mogulId).username();
		SecurityContextHolder.getContext()
			.setAuthentication(UsernamePasswordAuthenticationToken.authenticated(username, null, List.of()));
		// far enough out that nothing real is in the way
		var day = Instant.parse("2099-06-15T12:00:00Z");
		var status = mogulStatusService.forDate(mogulId, LocalDate.of(2099, 6, 15));
		var ids = new ArrayList<Long>();
		try {
			assertEquals(status.id(), mogulStatusService.forDate(mogulId, LocalDate.of(2099, 6, 15)).id(),
					"asking for the same day twice finds the same status");

			var published = this.publication(db, textEncryptor, mogulId, status, day.minus(Duration.ofDays(30)), day,
					null);
			var scheduled = this.publication(db, textEncryptor, mogulId, status, day.minus(Duration.ofDays(30)), null,
					day.plus(Duration.ofHours(1)));
			var outside = this.publication(db, textEncryptor, mogulId, status, day.minus(Duration.ofDays(30)), null,
					day.plus(Duration.ofDays(3)));
			ids.addAll(List.of(published, scheduled, outside));

			var found = publicationService.getPublicationsBetween(mogulId, day.minus(Duration.ofDays(1)),
					day.plus(Duration.ofDays(1)));
			var foundIds = found.stream().map(Publication::id).toList();
			assertEquals(List.of(published, scheduled), foundIds,
					"the published and the scheduled one, in the order they go out, and not the one due later");

			var references = publicationService.getPublishableReferences(found);
			for (var id : foundIds) {
				var reference = references.get(id);
				assertNotNull(reference, "publication " + id + " should say what it's about");
				assertEquals("mogulStatus", reference.type());
				assertEquals(status.id(), reference.id());
				assertEquals("2099-06-15", reference.title());
			}
		} //
		finally {
			for (var id : ids)
				db.sql("delete from publication where id = ?").params(id).update();
			db.sql("delete from mogul_status where id = ?").params(status.id()).update();
			SecurityContextHolder.clearContext();
		}
	}

	private Long publication(JdbcClient db, TextEncryptor textEncryptor, Long mogulId, MogulStatus status,
			Instant created, Instant published, Instant scheduled) {
		return db
			.sql("""
					insert into publication(state, mogul_id, plugin, created, published, context, payload, payload_class, scheduled)
					values (?,?,?,?,?,?,?,?,?) returning id
					""")
			.params((published == null ? Publication.State.PENDING : Publication.State.PUBLISHED).name(), mogulId,
					"test-plugin", Timestamp.from(created), published == null ? null : Timestamp.from(published),
					textEncryptor.encrypt("{}"), Long.toString(status.id()), MogulStatus.class.getName(),
					scheduled == null ? null : Timestamp.from(scheduled))
			.query((rs, _) -> rs.getLong("id"))
			.single();
	}

}
