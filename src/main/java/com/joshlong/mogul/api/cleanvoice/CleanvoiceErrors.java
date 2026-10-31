package com.joshlong.mogul.api.cleanvoice;

import org.springframework.web.client.HttpClientErrorException;

/**
 * what a failure from cleanvoice means for the mogul.
 */
abstract class CleanvoiceErrors {

	record Diagnosis(String reason, boolean actionRequired) {
	}

	static Diagnosis diagnose(Throwable throwable) {
		if (throwable instanceof CleanvoiceNotConfiguredException)
			return new Diagnosis("there's no cleanvoice api key in your settings", true);
		if (throwable instanceof HttpClientErrorException.Unauthorized
				|| throwable instanceof HttpClientErrorException.Forbidden)
			return new Diagnosis("cleanvoice didn't accept your api key", true);
		if (throwable instanceof HttpClientErrorException http && http.getStatusCode().value() == 402)
			return new Diagnosis("your cleanvoice account is out of credit", true);
		return new Diagnosis(
				throwable.getMessage() == null ? throwable.getClass().getSimpleName() : throwable.getMessage(), false);
	}

}
