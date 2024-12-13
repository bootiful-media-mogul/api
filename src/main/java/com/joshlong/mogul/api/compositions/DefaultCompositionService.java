package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.utils.JdbcUtils;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;

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
	public <T extends Composable> Composition compose(T payload, String field) {

		var generatedKeyHolder = new GeneratedKeyHolder();
		var key = JsonUtils.write(payload.compositionKey());
		var clazz = payload.getClass().getName();
		this.db //
			.sql("""
					insert into composition (  payload, payload_class ,   field ) values (?,?,?)
					on conflict on constraint composition_payload_class_payload_field_key
					do nothing
					""")//
			.params(key, clazz, field)//
			.update(generatedKeyHolder);

		return this.db.sql("select * from composition where  payload_class  = ? and payload = ?  and field = ? ")
			.params(clazz, key, field)
			.query(this.compositionRowMapper)
			.single();
	}

	@Override
	public Attachment attach(Long compositionId, String key, Long managedFileId) {
		var gkh = new GeneratedKeyHolder();
		this.db.sql("""
				insert into composition_attachment ( caption, composition_id, managed_file_id) values (?,?,?)
				""")//
			.params(key, compositionId, managedFileId)//
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
