package com.joshlong.mogul.api.publications;

/**
 * what a publication was about, in enough detail to list it beside publications of other
 * kinds: the publishable's type (as the client names it), its id, and a title. the title
 * is null when the publishable no longer exists.
 */
public record PublishableReference(String type, Long id, String title) {
}
