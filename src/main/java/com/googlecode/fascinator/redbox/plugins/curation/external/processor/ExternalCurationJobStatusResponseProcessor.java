package com.googlecode.fascinator.redbox.plugins.curation.external.processor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.googlecode.fascinator.api.indexer.Indexer;
import com.googlecode.fascinator.api.storage.Storage;
import com.googlecode.fascinator.common.BasicHttpClient;
import com.googlecode.fascinator.common.FascinatorHome;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.googlecode.fascinator.dao.GenericDao;
import com.googlecode.fascinator.portal.process.Processor;
import com.googlecode.fascinator.redbox.plugins.curation.external.PublicationHandler;
import com.googlecode.fascinator.redbox.plugins.curation.external.RelationshipMapper;
import com.googlecode.fascinator.redbox.plugins.curation.external.dao.model.CurationJob;
import com.googlecode.fascinator.spring.ApplicationContextProvider;

public class ExternalCurationJobStatusResponseProcessor implements Processor {

	/** Logging **/
	private Logger log = LoggerFactory.getLogger(RelationshipMapper.class);

	/** System configuration */
	private JsonSimpleConfig systemConfig;

	private PublicationHandler publicationHandler;

	public ExternalCurationJobStatusResponseProcessor() throws IOException {
		publicationHandler = (PublicationHandler) ApplicationContextProvider
				.getApplicationContext().getBean("publicationHandler");
		systemConfig = new JsonSimpleConfig();

	}

	@Override
	public boolean process(String id, String inputKey, String outputKey,
			String stage, String configFilePath, HashMap<String, Object> dataMap)
			throws Exception {
		if ("main".equals(stage)) {
			GenericDao<CurationJob, Integer> curationJobDao = (GenericDao<CurationJob, Integer>) ApplicationContextProvider
					.getApplicationContext().getBean("curationJobDao");
			List<CurationJob> jobs = curationJobDao.query("findInProgressJobs",
					new HashMap());
			for (CurationJob curationJob : jobs) {
				JsonSimple jobStatus = queryJobStatus(curationJob);

				String status = jobStatus.getString("FAILED", "jobStatus");
				writeResponseToStatusResponseCache(
						jobStatus.getInteger(null, "jobId"), jobStatus);				
				if ("COMPLETED".equals(status)) {
					publicationHandler.publishRecords(jobStatus
							.getArray("jobItems"));
					curationJob.setStatus(status);
					curationJobDao.create(curationJob);
				} else if ("FAILED".equals(status)) {
					curationJob.setStatus(status);
					curationJobDao.create(curationJob);
				}

			}
		}
		return true;
	}

	private void writeResponseToStatusResponseCache(Integer jobId,
			JsonSimple jobStatus) throws IOException {
		File curationStatusRespones = new File(FascinatorHome.getPath()
				+ "/curation-status-responses");
		if (curationStatusRespones.exists()) {
			FileUtils.forceMkdir(curationStatusRespones);
		}

		FileUtils.writeStringToFile(new File(curationStatusRespones.getPath()
				+ "/" + jobId + ".json"), jobStatus.toString(true));

	}

	private JsonSimple queryJobStatus(CurationJob curationJob)
			throws IOException {
		List<JsonObject> relations = new ArrayList<JsonObject>();
		GetMethod get;
		try {
			String url = systemConfig.getString(null, "curation",
					"curation-manager-url");
			BasicHttpClient client = new BasicHttpClient(url + "/job/"
					+ curationJob.getCurationJobId());
			get = new GetMethod(url+ "/job/"
					+ curationJob.getCurationJobId());
			client.executeMethod(get);
			int status = get.getStatusCode();
			if (status != 200) {
				String text = get.getStatusText();
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
			response = get.getResponseBodyAsString();
		} catch (IOException ex) {
			log.error("Error accessing response body: ", ex);
			return null;
		}

		return new JsonSimple(response);
	}

}
