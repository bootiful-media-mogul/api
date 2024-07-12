package com.joshlong.mogul.api;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Settings {

	private final JdbcClient db;

	private final TextEncryptor encryptor;

	private final SettingsRowMapper rowMapper;

	public Settings(JdbcClient db, TextEncryptor encryptor) {
		this.db = db;
		this.encryptor = encryptor;
		Assert.notNull(this.encryptor, "the encryptor must be non-null");
		Assert.notNull(this.db, "the db must be non-null");
		this.rowMapper = new SettingsRowMapper(encryptor);
	}

	public record Setting(String category, String key, String value) {
	}

	private static class SettingsRowMapper implements RowMapper<Setting> {

		private final TextEncryptor encryptor;

		SettingsRowMapper(TextEncryptor encryptor) {
			this.encryptor = encryptor;
			Assert.notNull(this.encryptor, "the " + TextEncryptor.class.getName() + " must be non-null");
		}

		@Override
		public Setting mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Setting(rs.getString("category"), rs.getString("key"), encryptor.decrypt(rs.getString("value")));
		}

	}

	public Map<String, String> getAllValuesByCategory(Long mogulId, String category) {
		var all = getAllSettingsByCategory(mogulId, category);
		var res = new HashMap<String, String>();
		for (var a : all.keySet())
			res.put(a, all.get(a).value());
		return res;
	}

	public Map<String, Setting> getAllSettingsByCategory(Long mogulId, String category) {

		var settings = this.db//
			.sql("select * from settings where mogul_id = ? and category =?   ")
			.params(mogulId, category)
			.query(this.rowMapper)
			.list();

		var map = new HashMap<String, Setting>();

		for (var s : settings)
			map.put(s.key(), s);

		return map;
	}

	private Setting get(Long mogulId, String category, String key) {
		var settings = this.db//
			.sql("select * from settings where mogul_id = ? and category =? and key = ? ")
			.params(mogulId, category, key)
			.query(this.rowMapper)
			.list();
		Assert.state(settings.size() <= 1, "there should never be more than one setting configured.");
		return settings.isEmpty() ? null : settings.getFirst();
	}

	public String getValue(Long mogulId, String category, String key) {
		var v = get(mogulId, category, key);
		if (v != null)
			return v.value();
		return null;
	}

	public void set(Long mogulId, String category, String key, String value) {
		this.db.sql("""
				insert into settings(mogul_id, category, key, value)
				values (? ,? ,? ,? )
				on conflict on constraint  settings_mogul_id_category_key_key do update set value = excluded.value
						    """) //
			.params(mogulId, category, key, this.encryptor.encrypt(value))//
			.update();

	}

}
