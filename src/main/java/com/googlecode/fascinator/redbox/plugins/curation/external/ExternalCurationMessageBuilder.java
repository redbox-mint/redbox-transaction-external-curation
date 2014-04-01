package com.googlecode.fascinator.redbox.plugins.curation.external;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.text.StrSubstitutor;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.storage.StorageUtils;
import com.googlecode.fascinator.dao.GenericDao;
import com.googlecode.fascinator.redbox.plugins.curation.external.dao.model.CurationJob;

@Component
public class ExternalCurationMessageBuilder {

	/** Logging **/
	private Logger log = LoggerFactory.getLogger(RelationshipMapper.class);

	/** Storage */
	@Autowired
	@Qualifier(value = "fascinatorStorage")
	private Storage storage;

	/** Indexer */
	@Autowired
	@Qualifier(value = "fascinatorIndexer")
	private Indexer indexer;

	@Autowired
	@Qualifier(value = "curationJobDao")
	private GenericDao<CurationJob, Integer> curationJobDao;
	
	private static final String MINT_DATA_PAYLOAD_NAME = "metadata.json";

	/** Data payload */
	private static String DATA_PAYLOAD_SUFFIX = ".tfpackage";

	@SuppressWarnings("unchecked")
	public JsonSimple buildMessage(Map<String, JsonObject> relationships)
			throws StorageException, IOException {
		JsonObject messageObject = new JsonObject();
		JSONArray records = new JSONArray();
		for (String oid : relationships.keySet()) {
			JsonObject recordData = new JsonObject();
			JsonObject relationship = relationships.get(oid);
			if (relationship.get("id") != null) {
				JsonSimple metadataJson = getDataFromStorage(oid);
				recordData.put("metadata", metadataJson.getJsonObject());

				Properties objMeta = storage.getObject(oid).getMetadata();
				String jsonConfigOid = objMeta.getProperty("jsonConfigOid");
				Payload jsonConfigPayload = StorageUtils.getPayload(storage,
						jsonConfigOid, objMeta.getProperty("jsonConfigPid"));
				JsonSimple jsonConfig = new JsonSimple(jsonConfigPayload.open());
				boolean alreadyCurated = jsonConfig.getBoolean(true,
						"curation", "alreadyCurated");

				if (!alreadyCurated) {
					JsonObject record = createRecordEntry(oid, metadataJson,
							jsonConfig);
					records.add(record);
				}
			} else {
				records.add(relationship);
			}
		}
		messageObject.put("records", records);
		return new JsonSimple(messageObject);
	}

	private JsonObject createRecordEntry(String oid, JsonSimple metadataJson,
			JsonSimple jsonConfig) {
		Map<String, String> valuesMap = buildStrSubstitutorValuesMap(metadataJson);
		StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

		JsonObject record = new JsonObject();
		record.put("oid", oid);
		JsonObject generalMappings = jsonConfig.getObject("curation",
				"identifierDataMapping", "general");
		if (generalMappings != null) {
			for (Object keyObject : generalMappings.keySet()) {
				record.put(keyObject,
						substitutor.replace(generalMappings.get(keyObject)));
			}
		}

		JSONArray requiredIdentifiers = jsonConfig.getArray("curation",
				"requiredIdentifiers");
		// recordData.put("requiredIdentifiers", requiredIdentifiers);
		JSONArray curationMessageRequiredIdentifiers = new JSONArray();
		for (Object requiredIdentifierObject : requiredIdentifiers) {
			JsonObject curationMessageRequiredIdentifierInfo = new JsonObject();
			String requiredIdentifier = (String) requiredIdentifierObject;
			curationMessageRequiredIdentifierInfo.put("identifier_type",
					requiredIdentifier);

			JsonObject requiredIdentifierMappings = jsonConfig.getObject(
					"curation", "identifierDataMapping", requiredIdentifier);
			if (requiredIdentifierMappings != null) {
				JsonObject curationMessageRequiredIdentifierMetaData = new JsonObject();

				for (Object keyObject : requiredIdentifierMappings.keySet()) {
					String mappingDescriptor = (String) requiredIdentifierMappings
							.get(keyObject);
					curationMessageRequiredIdentifierMetaData.put(keyObject,
							substitutor.replace(mappingDescriptor));
				}
				curationMessageRequiredIdentifierInfo.put("metadata",
						curationMessageRequiredIdentifierMetaData);
			}
			curationMessageRequiredIdentifiers
					.add(curationMessageRequiredIdentifierInfo);
		}
		record.put("required_identifiers", curationMessageRequiredIdentifiers);

		return record;
	}

	private Map<String, String> buildStrSubstitutorValuesMap(
			JsonSimple metadataJson) {
		Map<String, String> values = new HashMap<String, String>();
		JsonObject obj = metadataJson.getJsonObject();
		for (Object o : obj.keySet()) {
			String key = (String) o;
			Object value = obj.get(key);
			if (value instanceof String) {
				values.put(key, (String) value);
			} else if (value instanceof JsonObject) {
				processJsonObjectToFlatMap((JsonObject) value, key, values);
			} else {
				if(value != null) {
					values.put(key, value.toString());
				}
			}
		}
		return values;
	}

	private void processJsonObjectToFlatMap(JsonObject jsonObject,
			String parentKey, Map<String, String> valuesMap) {
		for (Object o : jsonObject.keySet()) {
			String key = (String) o;
			Object value = jsonObject.get(key);
			if (value instanceof String) {
				valuesMap.put(parentKey + "." + key, (String) value);
			} else if (value instanceof JsonObject) {
				processJsonObjectToFlatMap((JsonObject) value, parentKey + "."
						+ key, valuesMap);
			} else {
				valuesMap.put(parentKey + "." + key, value.toString());
			}
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

	public void saveJob(CurationJob job) {
		curationJobDao.create(job);
	}

}
