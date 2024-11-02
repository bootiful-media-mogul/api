package com.joshlong.mogul.api.feeds;

import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;

/**
 * Adapter to create ATOM 1.0-compliant and public feeds for content in Mogul.
 *
 * @author Josh Long
 */
@Component
public class Feeds {

	private static final String MOGUL_FEEDS_NS = "http://api.media-mogul.io";

	private static final String MOGUL_FEEDS_ELEMENT_PREFIX = "mogul-feeds";

	public <T> String createMogulAtomFeed(String feedTitle, String feedUrl, Instant published, String feedAuthor,
			String feedId, Collection<T> entries, EntryMapper<T> entryMapper)
			throws TransformerException, IOException, ParserConfigurationException {

		var docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);

		var docBuilder = docFactory.newDocumentBuilder();
		var doc = docBuilder.newDocument();

		var atomNamespace = "http://www.w3.org/2005/Atom";

		var feedElement = doc.createElementNS(atomNamespace, "feed");
		feedElement.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:mogul-feeds", MOGUL_FEEDS_NS);
		doc.appendChild(feedElement);

		var title = doc.createElementNS(atomNamespace, "title");
		title.setTextContent(feedTitle);
		feedElement.appendChild(title);

		var self = doc.createElementNS(atomNamespace, "link");
		self.setAttribute("rel", "self");
		self.setAttribute("href", feedUrl);
		feedElement.appendChild(self);

		var updated = doc.createElementNS(atomNamespace, "updated");
		var publishedString = formatInstant(published);
		updated.setTextContent(publishedString);
		feedElement.appendChild(updated);

		var author = doc.createElementNS(atomNamespace, "author");
		var authorName = doc.createElementNS(atomNamespace, "name");
		authorName.setTextContent(feedAuthor);
		author.appendChild(authorName);
		feedElement.appendChild(author);

		var id = doc.createElementNS(atomNamespace, "id");
		id.setTextContent("urn:uuid:" + feedId);
		feedElement.appendChild(id);
		entries.forEach(entry -> {
			try {
				var entryObj = entryMapper.map(entry);
				var entryElement = createEntry(doc, entryObj.id(), entryObj.updated(), entryObj.image().url(),
						entryObj.image().contentType(), entryObj.image().length(), entryObj.title(), entryObj.url(),
						entryObj.summary(), entryObj.metadata());
				feedElement.appendChild(entryElement);
			} //
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		var transformerFactory = TransformerFactory.newInstance();
		var transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

		var source = new DOMSource(doc);
		try (var writer = new StringWriter()) {
			var result = new StreamResult(writer);
			transformer.transform(source, result);
			return writer.toString();
		}
	}

	private static String formatInstant(Instant instant) {
		return DateTimeFormatter.ISO_INSTANT.format(instant.truncatedTo(ChronoUnit.SECONDS));
	}

	private static Element createEntry(Document doc, String entryId, Instant updatedInstant, String imageUrl,
			String imageContentType, long thumbnailSize, String titleTxt, String entryUrl, String summaryText,
			Map<String, String> customMetadataMap) throws ParserConfigurationException {
		var entry = doc.createElementNS("http://www.w3.org/2005/Atom", "entry");

		var enclosure = doc.createElementNS("http://www.w3.org/2005/Atom", "link");
		enclosure.setAttribute("rel", "enclosure");
		enclosure.setAttribute("type", imageContentType);
		enclosure.setAttribute("href", imageUrl);
		enclosure.setAttribute("title", "Thumbnail");
		enclosure.setAttribute("length", Long.toString(thumbnailSize));
		entry.appendChild(enclosure);

		// Add entry elements
		var title = doc.createElementNS("http://www.w3.org/2005/Atom", "title");
		title.setTextContent(titleTxt);
		entry.appendChild(title);

		var link = doc.createElementNS("http://www.w3.org/2005/Atom", "link");
		link.setAttribute("href", entryUrl);
		// link.setAttribute("rel", "self");
		entry.appendChild(link);

		var id = doc.createElementNS("http://www.w3.org/2005/Atom", "id");
		id.setTextContent("urn:uuid:" + entryId);
		entry.appendChild(id);

		var updated = doc.createElementNS("http://www.w3.org/2005/Atom", "updated");
		updated.setTextContent(formatInstant(updatedInstant));
		entry.appendChild(updated);

		var summary = doc.createElementNS("http://www.w3.org/2005/Atom", "summary");
		summary.setTextContent(summaryText);
		entry.appendChild(summary);

		var customMetadata = doc.createElementNS(MOGUL_FEEDS_NS, MOGUL_FEEDS_ELEMENT_PREFIX + ":metadata");

		customMetadataMap.forEach((key, value) -> {
			var element = doc.createElementNS(MOGUL_FEEDS_NS, MOGUL_FEEDS_ELEMENT_PREFIX + ":" + key);
			element.setTextContent(value);
			customMetadata.appendChild(element);
		});
		entry.appendChild(customMetadata);
		return entry;
	}

}