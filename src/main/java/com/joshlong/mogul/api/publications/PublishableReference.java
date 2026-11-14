package com.joshlong.mogul.api.publications;

/**
 * a publishable, named well enough to list beside publishables of other kinds.
 *
 * @param type the kind of publishable
 * @param id the publishable's id
 * @param title the publishable's title, or null if it has since been deleted
 */
public record PublishableReference(String type, Long id, String title) {
}
