package com.googlecode.fascinator.redbox.plugins.curation.external;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.indexer.IndexerException;
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
import com.googlecode.fascinator.common.storage.StorageUtils;

@Component
public class PublicationHandler {

	private static final String MINT_DATA_PAYLOAD_NAME = "metadata.json";

	/** Data payload */
	private static String DATA_PAYLOAD_SUFFIX = ".tfpackage";

	/** Logging **/
	private Logger log = LoggerFactory.getLogger(PublicationHandler.class);

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

	private String system;

	public PublicationHandler() throws IOException {
		systemConfig = new JsonSimpleConfig();
		system = systemConfig.getString("redbox", "system");
	}

	public void publishRecords(ArrayList<JsonObject> records) throws StorageException, IOException, IndexerException {

		for (JsonObject record : records) {
			String type = (String) record.get("type");
			String targetSystem = systemConfig.getString(null, "curation",
					"supported-types", type);
			if (targetSystem.equals(system)) {
				publishRecord(record);
			} else {
				publishRecordInExternalSystem(record,targetSystem);
			}

		}

	}

	private void publishRecordInExternalSystem(JsonObject record, String sourceSystem) {
		PostMethod post;
		try {
			String url = systemConfig.getString(null, "curation",
					"external-system-urls","publish", sourceSystem);
			BasicHttpClient client = new BasicHttpClient(url);
			post = new PostMethod(url);
			StringRequestEntity requestEntity = new StringRequestEntity(
				    new JsonSimple(record).toString(),
				    "application/json",
				    "UTF-8");
			post.setRequestEntity(requestEntity);
			client.executeMethod(post);
			int status = post.getStatusCode();
			if (status != 200) {
				String text = post.getStatusText();
				log.error(String
						.format("Error accessing Curation Manager, status code '%d' returned with message: %s",
								 status, text));
				log.error(String
						.format("Request message was: %s",
								 new JsonSimple(record).toString()));
				return;
			}

		} catch (IOException ex) {
			log.error("Error during search: ", ex);
			return;
		}

		// Return our results body
		String response = null;
		try {
			response = post.getResponseBodyAsString();
		} catch (IOException ex) {
			log.error("Error accessing response body: ", ex);
			return;
		}

	}

	private void publishRecord(JsonObject recordObject) throws StorageException, IOException, IndexerException {
		JsonSimple record = new JsonSimple(recordObject);
		String oid = (String)record.getString(null,"oid");
		DigitalObject object = storage.getObject(oid);
		Properties tfObjMeta = object.getMetadata();
		JSONArray requiredIdentifiers = record.getArray("required_identifiers");
		//Set all the pids as configured
		for (Object requiredIdentifierObject : requiredIdentifiers) {
			JsonSimple requiredIdentifier = new JsonSimple((JsonObject)requiredIdentifierObject);
			String identifierPid = systemConfig.getString(null,"curation","identifier-pids",requiredIdentifier.getString(null,"identifier_type"));
			tfObjMeta.put(identifierPid, requiredIdentifier.getString(null, "identifier"));
		}
		
		//Now publish the record
		tfObjMeta.put("published", "true");
		File tempFile = File.createTempFile("publication", "temporary");
		FileOutputStream outputStream = new FileOutputStream(tempFile);
		tfObjMeta.store(outputStream, null);
		
		FileInputStream inputStream = new FileInputStream(tempFile);
		StorageUtils.createOrUpdatePayload(object,"TF-OBJ-META",inputStream);
		tempFile.delete();
		indexer.index(oid);
	}

	
}
