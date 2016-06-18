package com.googlecode.fascinator.redbox.plugins.curation.external;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.methods.GetMethod;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.indexer.SearchRequest;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.api.transaction.TransactionException;
import com.googlecode.fascinator.common.BasicHttpClient;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.common.solr.SolrDoc;
import com.googlecode.fascinator.common.solr.SolrResult;

@Component
public class RelationshipMapper {

	private static final String MINT_DATA_PAYLOAD_NAME = "metadata.json";

	/** Data payload */
	private static String DATA_PAYLOAD_SUFFIX = ".tfpackage";

	/** Logging **/
	private Logger log = LoggerFactory
			.getLogger(RelationshipMapper.class);

	/** Storage */
	@Autowired
	@Qualifier(value = "fascinatorStorage")
	private Storage storage;

	/** Indexer */
	@Autowired
	@Qualifier(value = "fascinatorIndexer")
	private Indexer indexer;

	/** System configuration */
	private JsonSimpleConfig systemConfig;

	/** Relationship maps */
	private Map<String, JsonSimple> relationFields;

	private String system;

	public RelationshipMapper() throws IOException {
		systemConfig = new JsonSimpleConfig();
		/** Relationship mapping */
		relationFields = systemConfig.getJsonSimpleMap("curation", "relations");
		if (relationFields == null) {
			log.warn("Curation configuration has no relationships");
			relationFields = new HashMap<String, JsonSimple>();
		}

		system = systemConfig.getString("redbox", "system");
	}

	public Map<String, JsonObject> getRelationshipMap(String oid)
			throws IOException {
		Map<String, JsonObject> relationshipMap = new HashMap<String, JsonObject>();
		List<JsonObject> relations = getRelationshipsForObject(oid);
		for (JsonObject jsonObject : relations) {
		    String id = (String) (jsonObject.get("id") == null? jsonObject.get("oid") : jsonObject.get("id"));
			relationshipMap.put(id, jsonObject);
		}

		return relationshipMap;
	}

	public List<JsonObject> getRelationshipsForObject(String oid)
			throws IOException {
		List<JsonObject> relationshipList = new ArrayList<JsonObject>();
		JsonObject jsonObject = new JsonObject();
		jsonObject.put("id", oid);
		relationshipList.add(jsonObject);

		JSONArray relationships = mapRelations(oid);
		if (relationships != null) {
			for (Object object : relationships) {

				JsonSimple relationshipObject = new JsonSimple(
						(JsonObject) object);
				String sourceSystem = relationshipObject.getString(systemConfig.getString("redbox", "system"),
						"system");
				String relationOid = relationshipObject.getString(null, "oid");

				List<JsonObject> subRelationships;
				if (system.equals(sourceSystem)) {
					if(relationOid == null) {
						relationOid = findOidByIdentifier(relationshipObject.getString(null, "identifier"));
					}
					subRelationships = getRelationshipsForObject(relationOid);
				} else {
					if (relationOid != null) {
						subRelationships = getRelationsForObjectByOidFromExternalSystem(
								relationOid, sourceSystem);
					} else {
						subRelationships = getRelationsForObjectByExternalIdentifierFromExternalSystem(
								relationshipObject
										.getString(null, "identifier"),
								sourceSystem);
					}
				}

				relationshipList.addAll(subRelationships);

			}
		}

		return relationshipList;
	}

	private List<JsonObject> getRelationsForObjectByExternalIdentifierFromExternalSystem(
			String identifier, String sourceSystem) throws IOException {
		
		GetMethod get;
		try {
			String url = systemConfig.getString(null, "curation",
					"external-system-urls","relationships", sourceSystem);
			url = url + "&identifier=" + identifier;
			BasicHttpClient client = new BasicHttpClient(url);
			get = new GetMethod(url);
			client.executeMethod(get);
			int status = get.getStatusCode();
			if (status != 200) {
				String text = get.getStatusText();
				log.error(String
						.format("Error access external system %s, status code '%d' returned with message: %s",
								sourceSystem, status, text));
				return null;
			}

		} catch (IOException ex) {
			log.error("Error during search: ", ex);
			return null;
		}

		// Return our results body
		String response = null;
		try {
			response = get.getResponseBodyAsString();
		} catch (IOException ex) {
			log.error("Error accessing response body: ", ex);
			return null;
		}

		JsonSimple externalSystemResponseJson = new JsonSimple(response);
		
		return externalSystemResponseJson.getArray("records");
	}

	private List<JsonObject> getRelationsForObjectByOidFromExternalSystem(
			String relationOid, String sourceSystem) throws IOException {
		List<JsonObject> relations = new ArrayList<JsonObject>();
		GetMethod get;
		try {
			String url = systemConfig.getString(null, "curation",
					"external-system-urls", sourceSystem);
			BasicHttpClient client = new BasicHttpClient(url + "&oid="
					+ relationOid);
			get = new GetMethod(url);
			client.executeMethod(get);
			int status = get.getStatusCode();
			if (status != 200) {
				String text = get.getStatusText();
				log.error(String
						.format("Error access external system %s, status code '%d' returned with message: %s",
								sourceSystem, status, text));
				return null;
			}

		} catch (IOException ex) {
			log.error("Error during search: ", ex);
			return null;
		}

		// Return our results body
		String response = null;
		try {
			response = get.getResponseBodyAsString();
		} catch (IOException ex) {
			log.error("Error accessing response body: ", ex);
			return null;
		}

		JsonSimple externalSystemResponseJson = new JsonSimple(response);
		Collection<Object> values = externalSystemResponseJson.getJsonObject()
				.values();
		for (Object object : values) {
			relations.add((JsonObject) object);
		}
		return relations;
	}

	/**
	 * Map all the relationships buried in this record's data
	 * 
	 * @param oid
	 *            The object ID being curated
	 * @returns True is ready to proceed, otherwise False
	 */
	private JSONArray mapRelations(String oid) {
		// We want our parsed data for reading
		JsonSimple formData = parsedFormData(oid);
		if (formData == null) {
			// could be using Mint's ingest relationship parser
			JsonSimple rawData = getDataFromStorage(oid);
			if (rawData.getArray("relationships") != null) {
				JSONArray relations = rawData.getArray("relationships");
				for (Object relationship : relations) {
					((JsonObject) relationship).put("system", system);
				}
				return relations;

			}
			log.error("Error parsing form data");
			return null;
		}

		// And raw data to see existing relations and write new ones
		JsonSimple rawData = getDataFromStorage(oid);
		if (rawData == null) {
			log.error("Error reading data from storage");
			return null;
		}
		// Existing relationship data
		JSONArray relations = rawData.writeArray("relationships");
		boolean changed = false;

		// For all configured relationships
		for (String baseField : relationFields.keySet()) {
			JsonSimple relationConfig = relationFields.get(baseField);

			// Find the path we need to look under for our related object
			List<String> basePath = relationConfig.getStringList("path");
			if (basePath == null || basePath.isEmpty()) {
				log.error("Ignoring invalid relationship '{}'. No 'path'"
						+ " provided in configuration", baseField);
				continue;
			}

			// Get our base object
			Object object = formData.getPath(basePath.toArray());
			if (object instanceof JsonObject) {
				// And process it
				JsonObject newRelation = lookForRelation(oid, baseField,
						relationConfig, new JsonSimple((JsonObject) object));
				if (newRelation != null
						&& !isKnownRelation(relations, newRelation)) {
					log.info("Adding relation: '{}' => '{}'", baseField,
							newRelation.get("identifier"));
					relations.add(newRelation);
					changed = true;
				}
			}
			// If base path points at an array
			if (object instanceof JSONArray) {
				// Try every entry
				for (Object loopObject : (JSONArray) object) {
					if (loopObject instanceof JsonObject) {
						JsonObject newRelation = lookForRelation(oid,
								baseField, relationConfig, new JsonSimple(
										(JsonObject) loopObject));
						if (newRelation != null
								&& !isKnownRelation(relations, newRelation)) {
							log.info("Adding relation: '{}' => '{}'",
									baseField, newRelation.get("identifier"));
							relations.add(newRelation);
							changed = true;
						}
					}
				}
			}
		}

		// Do we need to store our object again?
		if (changed) {
			try {
				saveObjectData(rawData, oid);
			} catch (TransactionException ex) {
				log.error("Error updating object '{}' in storage: ", oid, ex);
				return null;
			}
		}

		return relations;
	}

	/**
	 * Look through part of the form data for a relationship.
	 * 
	 * @param oid
	 *            The object ID of the current object
	 * @param field
	 *            The full field String to store for comparisons
	 * @param config
	 *            The config relating to the relationship we are looking for
	 * @param baseNode
	 *            The JSON node the relationship should be under
	 * @return JsonObject A relationship in JSON, or null if not found
	 */
	private JsonObject lookForRelation(String oid, String field,
			JsonSimple config, JsonSimple baseNode) {
		JsonObject newRelation = new JsonObject();
		newRelation.put("field", field);
		newRelation.put("authority", true);

		// ** -1- ** EXCLUSIONS
		List<String> exPath = config.getStringList("excludeCondition", "path");
		String exValue = config.getString(null, "excludeCondition", "value");
		if (exPath != null && !exPath.isEmpty() && exValue != null) {
			String value = baseNode.getString(null, exPath.toArray());
			if (value != null && value.equals(exValue)) {
				log.info("Excluding relationship '{}' based on config", field);
				return null;
			}
		}
		String exStartsWith = config.getString(null, "excludeCondition",
				"startsWith");
		if (exPath != null && !exPath.isEmpty() && exStartsWith != null) {
			String value = baseNode.getString(null, exPath.toArray());
			if (value != null && value.startsWith(exStartsWith)) {
				log.info("Excluding relationship '{}' based on config", field);
				return null;
			}
		}

		// ** -2- ** IDENTIFIER
		// Inside that object where can we find the identifier
		List<String> idPath = config.getStringList("identifier");
		if (idPath == null || idPath.isEmpty()) {
			log.error("Ignoring invalid relationship '{}'. No 'identifier'"
					+ " provided in configuration", field);
			return null;
		}
		String id = baseNode.getString(null, idPath.toArray());
		if (id != null && !id.equals("")) {
			newRelation.put("identifier", id.trim());
		} else {
			log.info("Relationship '{}' has no identifier, ignoring!", field);
			return null;
		}

		// ** -3- ** RELATIONSHIP TYPE
		// Relationship type, it may be static and provided for us...
		String staticRelation = config.getString(null, "relationship");
		List<String> relPath = null;
		if (staticRelation == null) {
			// ... or it could be found in the form data
			relPath = config.getStringList("relationship");
		}
		// But we have to have one.
		if (staticRelation == null && (relPath == null || relPath.isEmpty())) {
			log.error("Ignoring invalid relationship '{}'. No relationship"
					+ " String of path in configuration", field);
			return null;
		}
		String relString = null;
		if (staticRelation != null) {
			relString = staticRelation;
		} else {
			relString = baseNode.getString("hasAssociationWith",
					relPath.toArray());
		}
		if (relString == null || relString.equals("")) {
			log.info("Relationship '{}' has no type, ignoring!", field);
			return null;
		}
		newRelation.put("relationship", relString);

		// ** -4- ** REVERSE RELATIONS
		String revRelation = systemConfig.getString("hasAssociationWith",
				"curation", "reverseMappings", relString);
		newRelation.put("reverseRelationship", revRelation);

		// ** -5- ** DESCRIPTION
		String description = config.getString(null, "description");
		if (description != null) {
			newRelation.put("description", description);
		}

		// ** -6- ** SYSTEM / BROKER
		String system = config.getString("mint", "system");
		newRelation.put("system", system);
		// ** -7- ** OPTIONAL
		boolean optional = config.getBoolean(false, "optional");
		if (optional) {
			newRelation.put("optional", optional);
		}

		return newRelation;
	}

	/**
	 * Get the form data from storage for the indicated object and parse it into
	 * a JSON structure.
	 * 
	 * @param oid
	 *            The object we want
	 */
	private JsonSimple parsedFormData(String oid) {
		// Get our data from Storage
		Payload payload = null;
		try {
			DigitalObject object = storage.getObject(oid);
			payload = getDataPayload(object);
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Parse the JSON
		try {
			try {
				return FormDataParser.parse(payload.open());
			} catch (Exception ex) {
				log.error("Error parsing data '{}': ", oid, ex);
				return null;
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing data '{}' in storage: ", oid, ex);
			return null;
		}
	}

	/**
	 * Get the stored data from storage for the indicated object.
	 * 
	 * @param oid
	 *            The object we want
	 */
	private JsonSimple getDataFromStorage(String oid) {
		// Get our data from Storage
		Payload payload = null;
		try {
			DigitalObject object = storage.getObject(oid);
			payload = getDataPayload(object);
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Parse the JSON
		try {
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing data '{}': ", oid, ex);
				return null;
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing data '{}' in storage: ", oid, ex);
			return null;
		}
	}

	/**
	 * Get the data payload (ending in '.tfpackage') from the provided object.
	 * 
	 * @param object
	 *            The digital object holding our payload
	 * @return Payload The payload requested
	 * @throws StorageException
	 *             if an errors occurs or the payload is not found
	 */
	private Payload getDataPayload(DigitalObject object)
			throws StorageException {
		for (String pid : object.getPayloadIdList()) {
			if (pid.endsWith(DATA_PAYLOAD_SUFFIX)
					|| pid.equals(MINT_DATA_PAYLOAD_NAME)) {
				return object.getPayload(pid);
			}
		}
		throw new StorageException("Data payload not found on storage!");
	}

	/**
	 * Test whether the field provided is already a known relationship
	 * 
	 * @param relations
	 *            The list of current relationships
	 * @param field
	 *            The cleaned field to provide some context
	 * @param id
	 *            The ID of the related object
	 * @returns True is it is a known relationship
	 */
	private boolean isKnownRelation(JSONArray relations, JsonObject newRelation) {
		// Does it have an OID? Highest priority. Avoids infinite loops
		// between ReDBox collections pointing at each other, so strict
		if (newRelation.containsKey("oid")) {
			for (Object relation : relations) {
				JsonObject json = (JsonObject) relation;
				String knownOid = (String) json.get("oid");
				String newOid = (String) newRelation.get("oid");
				// Do they match?
				if (knownOid.equals(newOid)) {
					log.debug("Known ReDBox linkage '{}'", knownOid);
					return true;
				}
			}
			return false;
		}

		// Mint records we are less strict about and happy
		// to allow multiple links in differing fields.
		for (Object relation : relations) {
			JsonObject json = (JsonObject) relation;

			if (json.containsKey("identifier")) {
				String knownId = (String) json.get("identifier");
				String knownField = (String) json.get("field");
				String newId = (String) newRelation.get("identifier");
				String newField = (String) newRelation.get("field");
				// And does the ID match?
				if (knownId.equals(newId) && knownField.equals(newField)) {
					return true;
				}
			}
		}
		// No match found
		return false;
	}

	/**
	 * Save the provided object data back into storage
	 * 
	 * @param data
	 *            The data to save
	 * @param oid
	 *            The object we want it saved in
	 */
	private void saveObjectData(JsonSimple data, String oid)
			throws TransactionException {
		// Get from storage
		DigitalObject object = null;
		try {
			object = storage.getObject(oid);
			getDataPayload(object);
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			throw new TransactionException(ex);
		}

		// Store modifications
		String jsonString = data.toString(true);
		try {
			updateDataPayload(object, jsonString);
		} catch (Exception ex) {
			log.error("Unable to store data '{}': ", oid, ex);
			throw new TransactionException(ex);
		}
	}

	/**
	 * Update the data payload (ending in '.tfpackage') in the provided object.
	 * 
	 * @param object
	 *            The digital object holding our payload
	 * @param input
	 *            The String to store
	 * @throws StorageException
	 *             if an errors occurs or the payload is not found
	 */
	private void updateDataPayload(DigitalObject object, String input)
			throws StorageException {
		try {
			for (String pid : object.getPayloadIdList()) {
				if (pid.endsWith(DATA_PAYLOAD_SUFFIX)) {
					InputStream inStream = new ByteArrayInputStream(
							input.getBytes("UTF-8"));
					object.updatePayload(pid, inStream);
					return;
				}
			}
			throw new StorageException("Data payload not found on storage!");
		} catch (Exception ex) {
			throw new StorageException("Error storing payload data!", ex);
		}
	}
	
	private String findOidByIdentifier(String identifier) {
		String query = "known_ids:\"" + identifier + "\"";
		SearchRequest request = new SearchRequest(query);
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		// Now search and parse response
		SolrResult result = null;
		try {
			indexer.search(request, out);
			InputStream in = new ByteArrayInputStream(out.toByteArray());
			result = new SolrResult(in);
		} catch (Exception ex) {
			log.error("Error searching Solr: ", ex);
			return null;
		}

		// Verify our results
		if (result.getNumFound() == 0) {
			log.error("Cannot resolve ID '{}'", identifier);
			return null;
		}
		if (result.getNumFound() > 1) {
			log.error("Found multiple OIDs for ID '{}'", identifier);
			return null;
		}

		// Return our result
		SolrDoc doc = result.getResults().get(0);
		return doc.getFirst("storage_id");

	}

}
