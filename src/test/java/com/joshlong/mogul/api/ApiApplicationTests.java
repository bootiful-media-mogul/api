package com.joshlong.mogul.api;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.docs.Documenter;

@SpringBootTest
class ApiApplicationTests {

	@Test
	void contextLoads() throws Exception {
		var am = ApplicationModules.of(ApiApplication.class);
		am.verify();
		System.out.println(am);
		new Documenter(am).writeDocumentation();
	}

}
