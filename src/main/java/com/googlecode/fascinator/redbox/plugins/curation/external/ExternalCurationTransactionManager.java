/* 
 * The Fascinator - ReDBox Curation Transaction Manager
 * Copyright (C) 2011-2012 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.redbox.plugins.curation.external;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
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

import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.PluginManager;
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
import com.googlecode.fascinator.common.transaction.GenericTransactionManager;
import com.googlecode.fascinator.messaging.EmailNotificationConsumer;
import com.googlecode.fascinator.messaging.TransactionManagerQueueConsumer;
import com.googlecode.fascinator.redbox.plugins.curation.external.dao.model.CurationJob;
import com.googlecode.fascinator.redbox.plugins.curation.external.dao.model.CurationRecord;
import com.googlecode.fascinator.spring.ApplicationContextProvider;


/**
 * Implements curation boundary logic for ReDBox. This class is also a
 * replacement for the standard tool chain.
 * 
 * @author Andrew Brazzatti
 */
public class ExternalCurationTransactionManager extends GenericTransactionManager {

	/** Data payload */
	private static String DATA_PAYLOAD_SUFFIX = ".tfpackage";

	/** Workflow payload */
	private static String WORKFLOW_PAYLOAD = "workflow.metadata";

	/** Property to set flag for ready to publish */
	private static String READY_PROPERTY = "ready_to_publish";

	/** Property to set flag for publication allowed */
	private static String PUBLISH_PROPERTY = "published";

	/** Logging **/
	private static Logger log = LoggerFactory.getLogger(ExternalCurationTransactionManager.class);

	/** System configuration */
	private JsonSimpleConfig systemConfig;

	/** Storage */
	private Storage storage;

	/** Solr Index */
	private Indexer indexer;

	/** External URL base */
	private String urlBase;

	/** Curation staff email address */
	private String emailAddress;

	/** Property to store PIDs */
	private String pidProperty;

	/** Send emails or just curate? */
	private boolean manualConfirmation;

	/** URL for our AMQ broker */
	private String brokerUrl;

	/** URL for Mint's AMQ broker */
	private String mintBroker;

	/** Relationship maps */
	private Map<String, JsonSimple> relationFields;
	
	
	private RelationshipMapper relationshipMapper;

	private ExternalCurationMessageBuilder externalCurationMessageBuilder;
	
	

	/**
	 * Base constructor
	 * 
	 */
	public ExternalCurationTransactionManager() {
		super("curation-external", "ReDBox Curation Transaction Manager");
	}

	/**
	 * Initialise method
	 * 
	 * @throws TransactionException
	 *             if there was an error during initialisation
	 */
	@Override
	public void init() throws TransactionException {
		systemConfig = getJsonConfig();

		// Load the storage plugin
		String storageId = systemConfig.getString("file-system", "storage",
				"type");
		if (storageId == null) {
			throw new TransactionException("No Storage ID provided");
		}
		storage = PluginManager.getStorage(storageId);
		if (storage == null) {
			throw new TransactionException("Unable to load Storage '"
					+ storageId + "'");
		}
		try {
			storage.init(systemConfig.toString());
		} catch (PluginException ex) {
			log.error("Unable to initialise storage layer!", ex);
			throw new TransactionException(ex);
		}

		// Load the indexer plugin
		String indexerId = systemConfig.getString("solr", "indexer", "type");
		if (indexerId == null) {
			throw new TransactionException("No Indexer ID provided");
		}
		indexer = PluginManager.getIndexer(indexerId);
		if (indexer == null) {
			throw new TransactionException("Unable to load Indexer '"
					+ indexerId + "'");
		}
		try {
			indexer.init(systemConfig.toString());
		} catch (PluginException ex) {
			log.error("Unable to initialise indexer!", ex);
			throw new TransactionException(ex);
		}

		// External facing URL
		urlBase = systemConfig.getString(null, "urlBase");
		if (urlBase == null) {
			throw new TransactionException("URL Base in config cannot be null");
		}

		// Where should emails be sent?
		emailAddress = systemConfig.getString(null, "curation",
				"curationEmailAddress");
		if (emailAddress == null) {
			throw new TransactionException("An admin email is required!");
		}

		// Where are PIDs stored?
		pidProperty = systemConfig.getString(null, "curation", "pidProperty");
		if (pidProperty == null) {
			throw new TransactionException("An admin email is required!");
		}

		// Do admin staff want to confirm each curation?
		manualConfirmation = systemConfig.getBoolean(false, "curation",
				"curationRequiresConfirmation");

		// Find the address of our broker
		brokerUrl = systemConfig.getString(null, "messaging", "url");
		if (brokerUrl == null) {
			throw new TransactionException("Cannot find the message broker.");
		}

		/** Relationship mapping */
		relationFields = systemConfig.getJsonSimpleMap("curation", "relations");
		if (relationFields == null) {
			log.warn("Curation configuration has no relationships");
			relationFields = new HashMap<String, JsonSimple>();
		}
		
		relationshipMapper = (RelationshipMapper)ApplicationContextProvider.getApplicationContext().getBean("relationshipMapper");
		externalCurationMessageBuilder = (ExternalCurationMessageBuilder)ApplicationContextProvider.getApplicationContext().getBean("externalCurationMessageBuilder");
	
	}

	/**
	 * Shutdown method
	 * 
	 * @throws PluginException
	 *             if any errors occur
	 */
	@Override
	public void shutdown() throws PluginException {
		if (storage != null) {
			try {
				storage.shutdown();
			} catch (PluginException pe) {
				log.error("Failed to shutdown storage: {}", pe.getMessage());
				throw pe;
			}
		}
		if (indexer != null) {
			try {
				indexer.shutdown();
			} catch (PluginException pe) {
				log.error("Failed to shutdown indexer: {}", pe.getMessage());
				throw pe;
			}
		}
	}

	

	private String idToOid(String identifier) {
		// Build a query
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

	

	/**
	 * Get the requested object ready for publication. This would typically just
	 * involve setting a flag
	 * 
	 * @param message
	 *            The incoming message
	 * @param oid
	 *            The object identifier to publish
	 * @return JsonSimple The response object
	 * @throws TransactionException
	 *             If an error occurred
	 */
	private JsonSimple publish(JsonSimple message, String oid)
			throws TransactionException {
		log.debug("Publishing '{}'", oid);
		JsonSimple response = new JsonSimple();
		try {
			DigitalObject object = storage.getObject(oid);
			Properties metadata = object.getMetadata();
			// Already published?
			if (!metadata.containsKey(PUBLISH_PROPERTY)) {
				metadata.setProperty(PUBLISH_PROPERTY, "true");
				object.close();
				log.info("Publication flag set '{}'", oid);
				audit(response, oid, "Publication flag set");
			} else {
				log.info("Publication flag is already set '{}'", oid);
			}
		} catch (StorageException ex) {
			throw new TransactionException("Error setting publish property: ",
					ex);
		}

		// Make a final pass through the curation tool(s),
		// allows for external publication. eg. VITAL
		JsonSimple itemConfig = getConfigFromStorage(oid);
		if (itemConfig == null) {
			log.error("Error accessing item configuration!");
		} else {
			List<String> list = itemConfig.getStringList("transformer",
					"curation");

			if (list != null && !list.isEmpty()) {
				for (String id : list) {
					JsonObject order = newTransform(response, id, oid);
					JsonObject config = (JsonObject) order.get("config");
					JsonObject overrides = itemConfig.getObject(
							"transformerOverrides", id);
					if (overrides != null) {
						config.putAll(overrides);
					}
				}
			}
		}

		// Don't forget to publish children
		publishRelations(response, oid);
		return response;
	}

	/**
	 * Send out requests to all relations to publish
	 * 
	 * @param oid
	 *            The object identifier to publish
	 */
	private void publishRelations(JsonSimple response, String oid) {
		log.debug("Publishing Children of '{}'", oid);

		JsonSimple data = getDataFromStorage(oid);
		if (data == null) {
			log.error("Error accessing item data! '{}'", oid);
			emailObjectLink(response, oid,
					"An error occured publishing the related objects for this"
							+ " record. Please check the system logs.");
			return;
		}

		JSONArray relations = data.writeArray("relationships");
		for (Object relation : relations) {
			JsonSimple json = new JsonSimple((JsonObject) relation);
			String broker = json.getString(null, "broker");
			boolean localRecord = broker.equals(brokerUrl);
			String relatedId = json.getString(null, "identifier");

			// We need to find OIDs to match IDs (only for local records)
			String relatedOid = json.getString(null, "oid");
			if (relatedOid == null && localRecord) {
				String identifier = json.getString(null, "identifier");
				if (identifier == null) {
					log.error("NULL identifer provided!");
				}
				relatedOid = idToOid(identifier);
				if (relatedOid == null) {
					log.error("Cannot resolve identifer: '{}'", identifier);
				}
			}

			// We only publish downstream relations (ie. we are their authority)
			boolean authority = json.getBoolean(false, "authority");
			if (authority) {
				// Is this relationship using a curated ID?
				boolean isCurated = json.getBoolean(false, "isCurated");
				if (isCurated) {
					log.debug(" * Publishing '{}'", relatedId);
					// It is a local object
					if (localRecord) {
						createTask(response, relatedOid, "publish");

						// Or remote
					} else {
						JsonObject task = createTask(response, broker,
								relatedOid, "publish");
						// We won't know OIDs for remote systems
						task.remove("oid");
						task.put("identifier", relatedId);
					}
				} else {
					log.debug(" * Ignoring non-curated relationship '{}'",
							relatedId);
				}
			}
		}
	}

	/**
	 * Processing method
	 * 
	 * @param message
	 *            The JsonSimple message to process
	 * @return JsonSimple The actions to take in response
	 * @throws TransactionException
	 *             If an error occurred
	 */
	@Override
	public JsonSimple parseMessage(JsonSimple message)
			throws TransactionException {
		log.debug("\n{}", message.toString(true));

		// A standard harvest event
		JsonObject harvester = message.getObject("harvester");
		String repoType= message.getString("", "indexer", "params", "repository.type");
		if (harvester != null && !"Attachment".equalsIgnoreCase(repoType)) {
			try {
				String oid = message.getString(null, "oid");
				JsonSimple response = new JsonSimple();
				audit(response, oid, "Tool Chain");

				// Standard transformers... ie. not related to curation
				scheduleTransformers(message, response);

				// Solr Index
				JsonObject order = newIndex(response, oid);
				order.put("forceCommit", true);

				// Send a message back here
				createTask(response, oid, "clear-render-flag");
				return response;
			} catch (Exception ex) {
				throw new TransactionException(ex);
			}
		} else {
			log.debug("Is type attachment, ignoring...");
		}

		// It's not a harvest, what else could be asked for?
		String task = message.getString(null, "task");
		if (task != null) {
			String oid = message.getString(null, "oid");

			if (task.equals("workflow")) {
				JsonSimple response = new JsonSimple();

				String eventType = message.getString(null, "eventType");
				String newStep = message.getString(null, "newStep");
				// The workflow has altered data, run the tool chain
				if (newStep != null || eventType.equals("ReIndex")) {
					// For housekeeping, we need to alter the
					// Solr index fairly speedily
					boolean quickIndex = message
							.getBoolean(false, "quickIndex");
					if (quickIndex) {
						JsonObject order = newIndex(response, oid);
						order.put("forceCommit", true);
					}

					// send a copy to the audit log
					JsonObject order = newSubscription(response, oid);
					JsonObject audit = (JsonObject) order.get("message");
					audit.putAll(message.getJsonObject());

					// Then business as usual
					reharvest(response, message);

					if(workflowCompleted(newStep)){
						createTask(response, oid,"curation");
					}

					// A traditional Subscriber message for audit logs
				} else {
					JsonObject order = newSubscription(response, oid);
					JsonObject audit = (JsonObject) order.get("message");
					audit.putAll(message.getJsonObject());
				}
				return response;
			}

			// ######################
			// Start a reharvest for this object
			if (task.equals("reharvest")) {
				JsonSimple response = new JsonSimple();
				reharvest(response, message);
				return response;
			}

			// ######################
			// Tool chain, clear render flag
			if (task.equals("clear-render-flag")) {
				if (oid != null) {
					clearRenderFlag(oid);
				} else {
					log.error("Cannot clear render flag without an OID!");
				}
			}

			// ######################
			// Curation
			if (task.startsWith("curation")) {
				try {
					Map<String, JsonObject> relationships = relationshipMapper.getRelationshipMap(oid);
					CurationJob job = buildCurationJob(relationships);
					JsonSimple externalCurationMessage = externalCurationMessageBuilder.buildMessage(relationships);
					
					JsonSimple externalCurationResponse = createJobInExternalCurationManager(externalCurationMessage);
					if(externalCurationResponse != null) {
						String jobId = externalCurationResponse.getString(null, "job_id");
						if(jobId == null) {
							throw new TransactionException("Response from external curation manager was invalid :" + externalCurationResponse.toString(true));
						}
						job.setCurationJobId(jobId);
						externalCurationMessageBuilder.saveJob(job);
					}
				} catch (IOException e) {
					throw new TransactionException("Error in resolving relationships during curation",e);
				} catch (StorageException e) {
					throw new TransactionException("Error in resolving relationships during curation",e);
				}
			}

		}

		// Do nothing
		return new JsonSimple();
	}

	

	private CurationJob buildCurationJob(Map<String, JsonObject> relationships) {
		Set<CurationRecord> curationRecords = new HashSet<CurationRecord>();
		CurationJob curationJob = new CurationJob();
		for(String oid : relationships.keySet()) {
			CurationRecord curationRecord = new CurationRecord();
			curationRecord.setOid(oid);
			curationRecord.setCurationJob(curationJob);
			curationRecords.add(curationRecord);
		}
		curationJob.setCurationRecords(curationRecords);
		return curationJob;
	}

	/**
	 * Generate a fairly common list of orders to transform and index an object.
	 * This mirrors the traditional tool chain.
	 * 
	 * @param message
	 *            The response to modify
	 * @param message
	 *            The message we received
	 */
	private void reharvest(JsonSimple response, JsonSimple message) {
		String oid = message.getString(null, "oid");

		try {
			if (oid != null) {
				setRenderFlag(oid);

				// Transformer config
				JsonSimple itemConfig = getConfigFromStorage(oid);
				if (itemConfig == null) {
					log.error("Error accessing item configuration!");
					return;
				}
				itemConfig.getJsonObject().put("oid", oid);

				// Tool chain
				scheduleTransformers(itemConfig, response);
				JsonObject order = newIndex(response, oid);
				order.put("forceCommit", true);
				createTask(response, oid, "clear-render-flag");
			} else {
				log.error("Cannot reharvest without an OID!");
			}
		} catch (Exception ex) {
			log.error("Error during reharvest setup: ", ex);
		}
	}

	/**
	 * Generate an order to send an email to the intended recipient with a link
	 * to an object
	 * 
	 * @param response
	 *            The response to add an order to
	 * @param message
	 *            The message we want to send
	 */
	private void emailObjectLink(JsonSimple response, String oid, String message) {
		String link = urlBase + "default/detail/" + oid;
		String text = "This is an automated message from the ";
		text += "ReDBox Curation Manager.\n\n" + message;
		text += "\n\nYou can find this object here:\n" + link;
		email(response, oid, text);
	}

	/**
	 * Generate an order to send an email to the intended recipient
	 * 
	 * @param response
	 *            The response to add an order to
	 * @param message
	 *            The message we want to send
	 */
	private void email(JsonSimple response, String oid, String text) {
		JsonObject object = newMessage(response,
				EmailNotificationConsumer.LISTENER_ID);
		JsonObject message = (JsonObject) object.get("message");
		message.put("to", emailAddress);
		message.put("body", text);
		message.put("oid", oid);
	}

	/**
	 * Generate an order to add a message to the System's audit log
	 * 
	 * @param response
	 *            The response to add an order to
	 * @param oid
	 *            The object ID we are logging
	 * @param message
	 *            The message we want to log
	 */
	private void audit(JsonSimple response, String oid, String message) {
		JsonObject order = newSubscription(response, oid);
		JsonObject messageObject = (JsonObject) order.get("message");
		messageObject.put("eventType", message);
	}

	/**
	 * Generate orders for the list of normal transformers scheduled to execute
	 * on the tool chain
	 * 
	 * @param message
	 *            The incoming message, which contains the tool chain config for
	 *            this object
	 * @param response
	 *            The response to edit
	 * @param oid
	 *            The object to schedule for clearing
	 */
	private void scheduleTransformers(JsonSimple message, JsonSimple response) {
		String oid = message.getString(null, "oid");
		List<String> list = message.getStringList("transformer", "metadata");
		if (list != null && !list.isEmpty()) {
			for (String id : list) {
				JsonObject order = newTransform(response, id, oid);
				// Add item config to message... if it exists
				JsonObject itemConfig = message.getObject(
						"transformerOverrides", id);
				if (itemConfig != null) {
					JsonObject config = (JsonObject) order.get("config");
					config.putAll(itemConfig);
				}
			}
		}
	}

	/**
	 * Clear the render flag for objects that have finished in the tool chain
	 * 
	 * @param oid
	 *            The object to clear
	 */
	private void clearRenderFlag(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			Properties props = object.getMetadata();
			props.setProperty("render-pending", "false");
			object.close();
		} catch (StorageException ex) {
			log.error("Error accessing storage for '{}'", oid, ex);
		}
	}

	/**
	 * Set the render flag for objects that are starting in the tool chain
	 * 
	 * @param oid
	 *            The object to set
	 */
	private void setRenderFlag(String oid) {
		try {
			DigitalObject object = storage.getObject(oid);
			Properties props = object.getMetadata();
			props.setProperty("render-pending", "true");
			object.close();
		} catch (StorageException ex) {
			log.error("Error accessing storage for '{}'", oid, ex);
		}
	}

	/**
	 * Create a task. Tasks are basically just trivial messages that will come
	 * back to this manager for later action.
	 * 
	 * @param response
	 *            The response to edit
	 * @param oid
	 *            The object to schedule for clearing
	 * @param task
	 *            The task String to use on receipt
	 * @return JsonObject Access to the 'message' node of this task to provide
	 *         further details after creation.
	 */
	private JsonObject createTask(JsonSimple response, String oid, String task) {
		return createTask(response, null, oid, task);
	}

	/**
	 * Create a task. This is a more detailed option allowing for tasks being
	 * sent to remote brokers.
	 * 
	 * @param response
	 *            The response to edit
	 * @param broker
	 *            The broker URL to use
	 * @param oid
	 *            The object to schedule for clearing
	 * @param task
	 *            The task String to use on receipt
	 * @return JsonObject Access to the 'message' node of this task to provide
	 *         further details after creation.
	 */
	private JsonObject createTask(JsonSimple response, String broker,
			String oid, String task) {
		JsonObject object = newMessage(response,
				TransactionManagerQueueConsumer.LISTENER_ID);
		if (broker != null) {
			object.put("broker", broker);
		}
		JsonObject message = (JsonObject) object.get("message");
		message.put("task", task);
		message.put("oid", oid);
		return message;
	}

	/**
	 * Creation of new Orders with appropriate default nodes
	 * 
	 */
	private JsonObject newIndex(JsonSimple response, String oid) {
		JsonObject order = createNewOrder(response,
				TransactionManagerQueueConsumer.OrderType.INDEXER.toString());
		order.put("oid", oid);
		return order;
	}

	private JsonObject newMessage(JsonSimple response, String target) {
		JsonObject order = createNewOrder(response,
				TransactionManagerQueueConsumer.OrderType.MESSAGE.toString());
		order.put("target", target);
		order.put("message", new JsonObject());
		return order;
	}

	private JsonObject newSubscription(JsonSimple response, String oid) {
		JsonObject order = createNewOrder(response,
				TransactionManagerQueueConsumer.OrderType.SUBSCRIBER.toString());
		order.put("oid", oid);
		JsonObject message = new JsonObject();
		message.put("oid", oid);
		message.put("context", "Curation");
		message.put("eventType", "Sending test message");
		message.put("user", "system");
		order.put("message", message);
		return order;
	}

	private JsonObject newTransform(JsonSimple response, String target,
			String oid) {
		JsonObject order = createNewOrder(response,
				TransactionManagerQueueConsumer.OrderType.TRANSFORMER
						.toString());
		order.put("target", target);
		order.put("oid", oid);
		JsonObject config = systemConfig.getObject("transformerDefaults",
				target);
		if (config == null) {
			order.put("config", new JsonObject());
		} else {
			order.put("config", config);
		}

		return order;
	}

	private JsonObject createNewOrder(JsonSimple response, String type) {
		JsonObject order = response.writeObject("orders", -1);
		order.put("type", type);
		return order;
	}

	/**
	 * Get the stored harvest configuration from storage for the indicated
	 * object.
	 * 
	 * @param oid
	 *            The object we want config for
	 */
	private JsonSimple getConfigFromStorage(String oid) {
		String configOid = null;
		String configPid = null;

		// Get our object and look for its config info
		try {
			DigitalObject object = storage.getObject(oid);
			Properties metadata = object.getMetadata();
			configOid = metadata.getProperty("jsonConfigOid");
			configPid = metadata.getProperty("jsonConfigPid");
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Validate
		if (configOid == null || configPid == null) {
			log.error("Unable to find configuration for OID '{}'", oid);
			return null;
		}

		// Grab the config from storage
		try {
			DigitalObject object = storage.getObject(configOid);
			Payload payload = object.getPayload(configPid);
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error accessing config '{}' in storage: ",
						configOid, ex);
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing object in storage: ", ex);
		}

		// Something screwed the pooch
		return null;
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
			if (pid.endsWith(DATA_PAYLOAD_SUFFIX)) {
				return object.getPayload(pid);
			}
		}
		throw new StorageException("Data payload not found on storage!");
	}

	private boolean workflowCompleted(String step) {
				if (step == null || !step.equals("live")) {
			log.debug("Workflow step '{}', ignoring.", step);
			return false;
		}

		return true;
	}
	
	/**
	 * Get the workflow data from the provided object.
	 * 
	 * @param object
	 *            The digital object holding our payload
	 * @return Payload The payload requested
	 * @throws StorageException
	 *             if an errors occurs or the payload is not found
	 */
	private JsonSimple getWorkflowData(String oid) {
		// Get our data from Storage
		Payload payload = null;
		try {
			DigitalObject object = storage.getObject(oid);
			payload = object.getPayload(WORKFLOW_PAYLOAD);
		} catch (StorageException ex) {
			log.error("Error accessing object '{}' in storage: ", oid, ex);
			return null;
		}

		// Parse the JSON
		try {
			try {
				return new JsonSimple(payload.open());
			} catch (IOException ex) {
				log.error("Error parsing workflow '{}': ", oid, ex);
				return null;
			} finally {
				payload.close();
			}
		} catch (StorageException ex) {
			log.error("Error accessing workflow '{}' in storage: ", oid, ex);
			return null;
		}
	}

	private JsonSimple createJobInExternalCurationManager(JsonSimple requestJson) throws IOException {
		
		PostMethod post;
		try {
			String url = systemConfig.getString(null, "curation",
					"curation-manager-url");
			url = url + "/job";
			BasicHttpClient client = new BasicHttpClient(url);
			post = new PostMethod(url);
			StringRequestEntity requestEntity = new StringRequestEntity(
				    requestJson.toString(),
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
				return null;
			}

		} catch (IOException ex) {
			log.error("Error during search: ", ex);
			return null;
		}

		// Return our results body
		String response = null;
		try {
			response = post.getResponseBodyAsString();
		} catch (IOException ex) {
			log.error("Error accessing response body: ", ex);
			return null;
		}

		JsonSimple curationManagerResponseJson = new JsonSimple(response);
		
		return curationManagerResponseJson;
	}
	
}
