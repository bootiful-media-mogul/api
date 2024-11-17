package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.utils.JdbcUtils;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Collection;

/**
 * helps to manage the lifecycle and entities associated with a given composition.
 */
@Service
class DefaultCompositionService implements CompositionService {

	private final JdbcClient db;

	private final ManagedFileService managedFileService;

	private final RowMapper<Composition> compositionRowMapper;

	private final RowMapper<Attachment> attachmentRowMapper;

	DefaultCompositionService(JdbcClient db, ManagedFileService managedFileService) {
		this.db = db;
		this.managedFileService = managedFileService;
		this.attachmentRowMapper = new AttachmentRowMapper(this.managedFileService::getManagedFile);
		this.compositionRowMapper = new CompositionRowMapper(this::getAttachmentsByComposition);
	}

	public Composition getCompositionById(Long id) {
		return this.db.sql("select  * from composition where id = ? ")
			.params(id)
			.query(this.compositionRowMapper)
			.single();
	}

	@Override
	public Composition compose(Long mogulId, Class<?> clazz, Serializable publicationKey, String field) {
		var generatedKeyHolder = new GeneratedKeyHolder();
		var payload = JsonUtils.write(publicationKey);
		var className = clazz.getName();
		this.db //
			.sql("""
					insert into composition ( mogul_id ,payload_class, field, payload) values (?,?,?,?)
					on conflict on constraint composition_mogul_id_payload_payload_class_field_key
					do nothing
					""")//
			.params(mogulId, className, field, payload)//
			.update(generatedKeyHolder);

		return this.db
			.sql("select * from composition where mogul_id = ? and payload_class = ? and field = ? and payload = ?")
			.params(mogulId, className, field, payload)
			.query(this.compositionRowMapper)
			.single();
	}

	@Override
	public Attachment attach(Long compositionId, String key, ManagedFile managedFile) {
		var gkh = new GeneratedKeyHolder();
		this.db.sql("""
				insert into composition_attachment ( key, composition_id, managed_file_id) values (?,?,?)
						""")//
			.params(key, compositionId, managedFile.id())//
			.update(gkh);
		var ai = JdbcUtils.getIdFromKeyHolder(gkh).longValue();
		return this.getAttachmentById(ai);
	}

	private Attachment getAttachmentById(Long id) {
		return this.db.sql("select * from composition_attachment where id = ?")
			.params(id)
			.query(this.attachmentRowMapper)
			.single();
	}

	private Collection<Attachment> getAttachmentsByComposition(Long compositionId) {
		return this.db.sql("select * from composition_attachment where composition_id = ?")
			.params(compositionId)
			.query(this.attachmentRowMapper)
			.list();
	}

}
