/** *****************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 * *****************************************************************************
 */
package fr.cirad.mgdb.importing;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.brapi.v2.model.Germplasm;
import org.brapi.v2.model.GermplasmAttributeValue;
import org.brapi.v2.model.GermplasmAttributeValueListResponse;
import org.brapi.v2.model.GermplasmListResponse;
import org.brapi.v2.model.Sample;
import org.brapi.v2.model.SampleListResponse;
import org.brapi.v2.model.SuccessfulSearchResponse;
import org.brapi.v2.model.SuccessfulSearchResponseResult;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.bulk.BulkWriteResult;

import fr.cirad.io.brapi.BrapiClient;
import fr.cirad.io.brapi.BrapiClient.Pager;
import fr.cirad.io.brapi.BrapiService;
import fr.cirad.io.brapi.BrapiV2Client;
import fr.cirad.io.brapi.BrapiV2Service;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata.CustomIndividualMetadataId;
import fr.cirad.mgdb.model.mongo.maintypes.CustomSampleMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.CustomSampleMetadata.CustomSampleMetadataId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.germplasm.BrapiGermplasmAttributes;
import jhi.brapi.api.samples.BrapiSample;
import jhi.brapi.api.search.BrapiSearchResult;
import retrofit2.Response;

/**
 * The Class IndividualMetadataImport.
 */
public class IndividualMetadataImport {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(IndividualMetadataImport.class);

    public static final String REF_TYPE_SAMPLE = "sample";
    public static final String REF_TYPE_GERMPLASM = "germplasm";
    public static final String BRAPI_FILTER_SAMPLE_IDS = "sampleDbIds";
    public static final String BRAPI_FILTER_GERMPLASM_IDS = "germplasmDbIds";
    
    public static final ObjectMapper mapper = new ObjectMapper();    
    
    static {
        SimpleModule module = new SimpleModule();
        mapper.registerModule(module);
        mapper.setSerializationInclusion(Include.NON_NULL);
    }    
    

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3)
            throw new Exception("You must pass 3 or 4 parameters as arguments: DATASOURCE name, metadata file path (TSV format with header on first line), label of column containing individual names (matching those in the DB), and optionally a CSV list of column labels for fields to import (all will be imported if no such parameter is supplied).");

        importIndividualOrSampleMetadata(args[0], null, new File(args[1]).toURI().toURL(), args[2], args.length > 3 ? args[3] : null, null);
    }

    public static int importIndividualOrSampleMetadata(String sModule, HttpSession session, URL metadataFileURL, String targetTypeColName, String csvFieldListToImport, String username) throws Exception {
        List<String> fieldsToImport = csvFieldListToImport != null ? Arrays.asList(csvFieldListToImport.toLowerCase().split(",")) : null;
        Scanner scanner = new Scanner(metadataFileURL.openStream());

        GenericXmlApplicationContext ctx = null;
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate == null) { // we are probably being invoked offline
            ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

            MongoTemplateManager.initialize(ctx);
            mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) {
                throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
            }
        }

        boolean fIsAnonymous = "anonymousUser".equals(username);
        LinkedHashMap<Comparable /*sample*/, LinkedHashMap<String, Object>> sessionObject = (LinkedHashMap<Comparable, LinkedHashMap<String, Object>>) session.getAttribute(targetTypeColName + "s_metadata_" + sModule);
        if (fIsAnonymous && sessionObject == null) {
            sessionObject = new LinkedHashMap<>();
            session.setAttribute(targetTypeColName + "s_metadata_" + sModule, sessionObject);
        }
        
        boolean fFlapjackFormat = metadataFileURL.getFile().toLowerCase().endsWith(".phenotype");

        try {
            HashMap<Integer, String> columnLabels = new HashMap<Integer, String>();
            int idColumn = -1;
            String sLine = null;
            
            BulkOperations bulkOperations;
            if (targetTypeColName.equals("individual"))
                bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
            else
                bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? GenotypingSample.class : CustomSampleMetadata.class);
            
            List<String> targetEntityList = new ArrayList<>();
            while (scanner.hasNextLine()) {
                sLine = scanner.nextLine();
                if (sLine.length() == 0 || (fFlapjackFormat && sLine.replaceAll("\\s+", "").equals("#fjFile=PHENOTYPE")))
                    continue;

                if (columnLabels.isEmpty() && sLine.startsWith("\uFEFF"))
                    sLine = sLine.substring(1);

                List<String> cells = Helper.split(sLine, "\t");

                if (columnLabels.isEmpty()) { // it's the header line
                    for (int i = 0; i < cells.size(); i++) {
                        String cell = cells.get(i);
                        if (cell.equalsIgnoreCase(targetTypeColName))
                            idColumn = i;
                        else if (!cell.isEmpty() && (fieldsToImport == null || fieldsToImport.contains(cell.toLowerCase())))
                            columnLabels.put(i, cell);
                    }
                    if (idColumn == -1) {
                    	if (!fFlapjackFormat || columnLabels.containsKey(0))
                            throw new Exception(cells.size() <= 1 ? "Provided file does not seem to be tab-delimited!" : "Unable to find individual name column \"" + targetTypeColName + "\" in file header!");

                    	idColumn = 0;	// FJ .phenotype file's field-name line starts with an empty string 
                    }
                    continue;
                }

                // now deal with actual data rows
                LinkedHashMap<String, Object> additionalInfo = new LinkedHashMap<>();
                for (int col : columnLabels.keySet())
                    if (col != idColumn)
                        additionalInfo.put(columnLabels.get(col), cells.size() > col ? cells.get(col) : "");

                String targetEntity = cells.get(idColumn);
                targetEntityList.add(targetEntity);
                
                Integer spId = null;	// will remain null if working on individuals
                if (targetTypeColName.equals("sample"))
                	try {
                		spId = mongoTemplate.findDistinct(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).is(targetEntity)), "_id", GenotypingSample.class, Integer.class).get(0);
                	}
                	catch (IndexOutOfBoundsException ioobe) {
                		throw new Exception("Unexisting sample: " + targetEntity);
                	}

                Update update = new Update();
                if (username == null) { // global metadata                    
                	additionalInfo.forEach((k, v) -> update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
                    bulkOperations.updateMulti(new Query(Criteria.where(spId != null ? GenotypingSample.FIELDNAME_NAME : "_id").is(targetEntity)), update);
                } else if (!fIsAnonymous) { // persistent user-level metadata
                   	additionalInfo.forEach((k, v) -> update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));
                   	if (spId != null)
                   		bulkOperations.upsert(new Query(new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_USER).is(username), new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_SAMPLE_ID).is(spId)))), update);
                   	else
                   		bulkOperations.upsert(new Query(new Criteria().andOperator(Criteria.where("_id." + CustomIndividualMetadataId.FIELDNAME_USER).is(username), new Criteria().andOperator(Criteria.where("_id." + CustomIndividualMetadataId.FIELDNAME_INDIVIDUAL_ID).is(targetEntity)))), update);
                } else {
                	LinkedHashMap<String, Object> existingEntityMetadata = sessionObject.get(spId == null ? targetEntity : spId);
                	if (existingEntityMetadata == null) {
                		existingEntityMetadata = new LinkedHashMap<>();
                		sessionObject.put(spId == null ? targetEntity : spId, existingEntityMetadata);
                	}
                	existingEntityMetadata.putAll(additionalInfo);
                }
            }

            if (targetEntityList.size() == 0) { // a blank metadata file was submitted: let's delete any existing metadata
                if (username == null) { // global metadata
                    bulkOperations.updateMulti(new Query(), new Update().unset(targetTypeColName.equals("sample") ? GenotypingSample.SECTION_ADDITIONAL_INFO : Individual.SECTION_ADDITIONAL_INFO));				
                } else if (!fIsAnonymous) {
                    if (targetTypeColName.equals("sample"))
                        bulkOperations.remove(new Query(Criteria.where("_id." + CustomSampleMetadata.CustomSampleMetadataId.FIELDNAME_USER).is(username)));
                    else
                        bulkOperations.remove(new Query(Criteria.where("_id." + CustomIndividualMetadata.CustomIndividualMetadataId.FIELDNAME_USER).is(username)));
                }
                else
                	sessionObject.clear();
            } else {	// first check if all referred objects actually exist in the DB
                if (targetTypeColName.equals("sample")) {
                    Query query = new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).in(targetEntityList));
                    List<String> foundSamples = mongoTemplate.find(query, GenotypingSample.class).stream().map(s -> s.getSampleName()).collect(Collectors.toList());
  
                    if (foundSamples.size() < targetEntityList.size())
                        throw new Exception("The following samples do not exist in the selected database: " + StringUtils.join(CollectionUtils.disjunction(targetEntityList, foundSamples), ", "));
                } else {
                    Query verificationQuery = new Query(Criteria.where("_id").in(targetEntityList));
                    verificationQuery.fields().include("_id");
                    List<String> foundIndList = mongoTemplate.find(verificationQuery, Individual.class).stream().map(ind -> ind.getId()).collect(Collectors.toList());
                    if (foundIndList.size() < targetEntityList.size())
                        throw new Exception("The following individuals do not exist in the selected database: " + StringUtils.join(CollectionUtils.disjunction(targetEntityList, foundIndList), ", "));
                }
            }
            if (!fIsAnonymous) {
                BulkWriteResult wr = bulkOperations.execute();
                if (targetEntityList.size() == 0)
                    LOG.info("Database " + sModule + ": metadata was deleted for " + wr.getModifiedCount() + " " + targetTypeColName + "s");
                else if (wr.getUpserts().size() > 0)
                	LOG.info("Database " + sModule + ": " + wr.getUpserts().size() + " custom " + targetTypeColName + " metadata records added");
                else
                    LOG.info("Database " + sModule + ": " + wr.getModifiedCount() + " " + targetTypeColName + "s updated with metadata, out of " + wr.getMatchedCount() + " matched documents");
                return wr.getModifiedCount() + wr.getUpserts().size() + wr.getDeletedCount();
            } else {
            	if (targetEntityList.size() == 0)
            		LOG.info("Database " + sModule + ": " + targetTypeColName + " metadata was deleted from session for anonymous user");
            	else
            		LOG.info("Database " + sModule + ": " + targetTypeColName + " metadata was persisted into session for anonymous user");
                return 1;
            }
        } finally {
            scanner.close();
            if (ctx != null)
                ctx.close();
        }
    }

    public static int importBrapiMetadata(String sModule, HttpSession session, String endpointUrl, HashMap<String, String> externalToInternalIdMap, String username, String authToken, ProgressIndicator progress, String metadataType) throws Exception {
        if (externalToInternalIdMap == null || externalToInternalIdMap.isEmpty()) {
            return 0;    // we must know which individuals to update
        }
        GenericXmlApplicationContext ctx = null;
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate == null) { // we are probably being invoked offline
            ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

            MongoTemplateManager.initialize(ctx);
            mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) {
                throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
            }
        }

        int modifiedCount = 0;

        String fixedEndpointUrl = !endpointUrl.endsWith("/") ? endpointUrl + "/" : endpointUrl;
        if (fixedEndpointUrl.endsWith("/brapi/v1/")) {
            if (metadataType.equals("individual")) {
                modifiedCount += importBrapiV1Germplasm(
                        sModule,
                        session,
                        endpointUrl,
                        externalToInternalIdMap,
                        username,
                        authToken,
                        progress,
                        mongoTemplate
                );
            } else if (metadataType.equals("sample")) {
                modifiedCount += importBrapiV1Samples(
                        sModule,
                        session,
                        endpointUrl,
                        externalToInternalIdMap,
                        username,
                        authToken,
                        progress,
                        mongoTemplate
                );
            }

        } else if (fixedEndpointUrl.endsWith("/brapi/v2/")) {
            if (metadataType.equals("individual")) {
                modifiedCount += importBrapiV2Germplasm(
                        sModule,
                        session,
                        endpointUrl,
                        externalToInternalIdMap,
                        username,
                        authToken,
                        progress,
                        mongoTemplate
                );
             } else if (metadataType.equals("sample")) {
                modifiedCount += importBrapiV2Samples(
                        sModule,
                        session,
                        endpointUrl,
                        externalToInternalIdMap,
                        username,
                        authToken,
                        progress,
                        mongoTemplate
                );
            }
        }

        return modifiedCount;
    }

    private static int importBrapiV1Germplasm(
            String sModule,
            HttpSession session,
            String endpointUrl,
            HashMap<String, String> germplasmDbIdToIndividualMap,
            String username,
            String authToken,
            ProgressIndicator progress,
            MongoTemplate mongoTemplate) throws Exception {

        BrapiClient client = new BrapiClient();
        client.initService(endpointUrl, authToken);
        client.getCalls();
        client.ensureGermplasmInfoCanBeImported();
        client.initService(endpointUrl, authToken);

        final BrapiService service = client.getService();

        progress.addStep("Getting germplasm information from " + endpointUrl);
        progress.moveToNextStep();

        // Retrieve germplasm and attributes information by calling Brapi services 
        boolean fCanQueryAttributes = client.hasCallGetAttributes();
        Map<String, Map<String, Object>> germplasmMap = getBrapiV1GermplasmWithAttributes(service, endpointUrl, germplasmDbIdToIndividualMap.keySet(), fCanQueryAttributes, progress);

        if (germplasmMap.keySet().isEmpty()) {
            return 0;
        }

        boolean fIsAnonymous = "anonymousUser".equals(username);
        LinkedHashMap<Comparable /*individual*/, LinkedHashMap<String, Object>> sessionObject = (LinkedHashMap<Comparable, LinkedHashMap<String, Object>>) session.getAttribute("individuals_metadata_" + sModule);
        if (fIsAnonymous && sessionObject == null) {
            sessionObject = new LinkedHashMap<>();
            session.setAttribute("individuals_metadata_" + sModule, sessionObject);
        }

        // Inserting germplasm and attributes information in DB as additional info
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
        int i = 0;
        for (String germplasmId : germplasmMap.keySet()) {
            Map<String, Object> aiMap = germplasmMap.get(germplasmId);

            progress.setCurrentStepProgress((long) (++i * 100f / germplasmMap.keySet().size()));

            if (aiMap.isEmpty()) {
                LOG.warn("Found no metadata to import for germplasm " + germplasmId);
                continue;
            }
            aiMap.put(BrapiService.BRAPI_FIELD_extGermplasmDbId, aiMap.remove(BrapiService.BRAPI_FIELD_germplasmDbId));		// use a dedicated field name to avoid confusion

            String indName = germplasmDbIdToIndividualMap.get(germplasmId);
            
            Update update = new Update();
            if (username == null) { // global metadata
                aiMap.forEach((k, v) -> update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
                bulkOperations.updateMulti(new Query(Criteria.where("_id").is(indName)), update);
            } else if (!fIsAnonymous) { // persistent user-level metadata
                aiMap.forEach((k, v) -> update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));
                bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(indName, username))), update);
            } else {
            	LinkedHashMap<String, Object> existingEntityMetadata = sessionObject.get(indName);
            	if (existingEntityMetadata == null) {
            		existingEntityMetadata = new LinkedHashMap<>();
            		sessionObject.put(indName, existingEntityMetadata);
            	}
            	existingEntityMetadata.putAll(aiMap);
            }
        }

        progress.addStep("Persisting metadata found at " + endpointUrl);
        progress.moveToNextStep();

        if (!fIsAnonymous) {
            BulkWriteResult wr = bulkOperations.execute();
            return wr.getModifiedCount() + wr.getUpserts().size();
        } else {
            LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
            return 1;
        }
    }

    private static int importBrapiV2Germplasm(
            String sModule,
            HttpSession session,
            String endpointUrl,
            HashMap<String, String> germplasmDbIdToIndividualMap,
            String username,
            String authToken,
            ProgressIndicator progress,
            MongoTemplate mongoTemplate) throws Exception {

        BrapiV2Client client = new BrapiV2Client();

        // hack to try and make it work with current BMS version
        client.initService(endpointUrl/*.replace("Ricegigwa/", "")*/, authToken);
        client.getCalls();
        client.ensureGermplasmInfoCanBeImported();
        client.initService(endpointUrl, authToken);

        final BrapiV2Service service = client.getService();

        // Retrieve germplasm and attributes information by calling Brapi services
        boolean fCanQueryAttributes = client.hasCallSearchAttributes();
        Map<String, Map<String, Object>> germplasmMap = getBrapiV2GermplasmWithAttributes(service, endpointUrl, germplasmDbIdToIndividualMap.keySet(), fCanQueryAttributes, progress);

        if (germplasmMap.keySet().isEmpty()) {
            return 0;
        }

        boolean fIsAnonymous = "anonymousUser".equals(username);
        LinkedHashMap<Comparable /*individual*/, LinkedHashMap<String, Object>> sessionObject = (LinkedHashMap<Comparable, LinkedHashMap<String, Object>>) session.getAttribute("individuals_metadata_" + sModule);
        if (fIsAnonymous && sessionObject == null) {
            sessionObject = new LinkedHashMap<>();
            session.setAttribute("individuals_metadata_" + sModule, sessionObject);
        }

        // Inserting germplasm and attributes information in DB as additional info
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
        int i = 0;
        for (String germplasmId : germplasmMap.keySet()) {
            Map<String, Object> aiMap = germplasmMap.get(germplasmId);

            progress.setCurrentStepProgress((long) (++i * 100f / germplasmMap.keySet().size()));

            if (aiMap.isEmpty()) {
                LOG.warn("Found no metadata to import for germplasm " + germplasmId);
                continue;
            }
            aiMap.put(BrapiService.BRAPI_FIELD_extGermplasmDbId, aiMap.remove(BrapiService.BRAPI_FIELD_germplasmDbId));		// use a dedicated field name to avoid confusion
            
        	String indName = germplasmDbIdToIndividualMap.get(germplasmId);

            Update update = new Update();
            if (username == null) { // global metadata
                aiMap.forEach((k, v) -> update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
                bulkOperations.updateMulti(new Query(Criteria.where("_id").is(indName)), update);
            } else if (!fIsAnonymous) { // persistent user-level metadata
                aiMap.forEach((k, v) -> update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));
                bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(indName, username))), update);
            } else {
            	LinkedHashMap<String, Object> existingEntityMetadata = sessionObject.get(indName);
            	if (existingEntityMetadata == null) {
            		existingEntityMetadata = new LinkedHashMap<>();
            		sessionObject.put(indName, existingEntityMetadata);
            	}
            	existingEntityMetadata.putAll(aiMap);
            }
        }

        progress.addStep("Persisting metadata found at " + endpointUrl);
        progress.moveToNextStep();

        if (!fIsAnonymous) {
            BulkWriteResult wr = bulkOperations.execute();
            return wr.getModifiedCount() + wr.getUpserts().size();
        } else {
            LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
            return 1;
        }
    }

    private static int importBrapiV1Samples(
            String sModule,
            HttpSession session,
            String endpointUrl,
            HashMap<String, String> externalToInternalIdMap,
            String username,
            String authToken,
            ProgressIndicator progress,
            MongoTemplate mongoTemplate) throws Exception {

        HashMap<String, Object> reqBody = new HashMap<>();
        reqBody.put(BRAPI_FILTER_SAMPLE_IDS, externalToInternalIdMap.keySet());

        BrapiClient client = new BrapiClient();

        // hack to try and make it work with current BMS version
        client.initService(endpointUrl/*.replace("Ricegigwa/", "")*/, authToken);
        client.getCalls();
        client.ensureSampleInfoCanBeImported();
        client.initService(endpointUrl, authToken);

        final BrapiService service = client.getService();

        List<BrapiSample> sampleList = new ArrayList<>();

        // Getting sample information by calling Brapi services
        if (client.hasCallSearchSamples()) {
            progress.addStep("Getting sample information from " + endpointUrl);
            progress.moveToNextStep();

            try {
                Response<BrapiBaseResource<BrapiSearchResult>> response = service.searchSamples(reqBody).execute();
                handleErrorCode(response.code());
                BrapiSearchResult bsr = response.body().getResult();

                BrapiClient.Pager samplePager = new BrapiClient.Pager();
                while (samplePager.isPaging()) {
                    BrapiListResource<BrapiSample> br = service.searchSamplesResult(bsr.getSearchResultsDbId(), samplePager.getPageSize(), samplePager.getPage()).execute().body();
                    sampleList.addAll(br.data());
                    samplePager.paginate(br.getMetadata());
                }
            } catch (Exception e) {    // we did not get a searchResultDbId: see if we actually got results straight away
                try {
                    Response<BrapiListResource<BrapiSample>> response = service.searchSamplesDirectResult(reqBody).execute();
                    handleErrorCode(response.code());
                    BrapiListResource<BrapiSample> br = response.body();

                    BrapiClient.Pager samplePager = new BrapiClient.Pager();
                    while (samplePager.isPaging()) {
                        sampleList.addAll(br.data());
                        samplePager.paginate(br.getMetadata());
                    }
                } catch (Exception f) {
                    progress.setError("Error invoking BrAPI /search/samples call (no searchResultDbId returned and yet unable to directly obtain results)");
                    LOG.error(e);
                    LOG.error(progress.getError(), f);
                    return 0;
                }
            }
        }

        progress.addStep("Importing sample information from " + endpointUrl);
        progress.moveToNextStep();

        if (sampleList.isEmpty()) {
            return 0;
        }

        boolean fIsAnonymous = "anonymousUser".equals(username);
        LinkedHashMap<Comparable /*sample*/, LinkedHashMap<String, Object>> sessionObject = (LinkedHashMap<Comparable, LinkedHashMap<String, Object>>) session.getAttribute("samples_metadata_" + sModule);
        if (fIsAnonymous && sessionObject == null) {
            sessionObject = new LinkedHashMap<>();
            session.setAttribute("samples_metadata_" + sModule, sessionObject);
        }

        // Inserting samples (also germplasm and attributes) information in DB as additional info
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? GenotypingSample.class : CustomSampleMetadata.class);
        for (int i = 0; i < sampleList.size(); i++) {
            BrapiSample sample = sampleList.get(i);
            Map<String, Object> aiMap = mapper.convertValue(sample, Map.class);
            //Map<String, Object> gMap = germplasmMap.get(sample.getGermplasmDbId());
            //merging 2 maps
            //aiMap.putAll(gMap);

            progress.setCurrentStepProgress((long) (i * 100f / sampleList.size()));

            if (aiMap.isEmpty()) {
                LOG.warn("Found no metadata to import for sample " + sample.getSampleDbId());
                continue;
            }
            aiMap.remove(BrapiService.BRAPI_FIELD_sampleDbId); // we don't want to persist this field as it's internal to the remote source but not to the present system

            String internalId = externalToInternalIdMap.get(sample.getSampleDbId());
            Integer spId = null;
        	try {
        		spId = mongoTemplate.findDistinct(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).is(internalId)), "_id",  GenotypingSample.class, Integer.class).get(0);
        	}
        	catch (IndexOutOfBoundsException ioobe) {
        		throw new Exception("Unexisting sample: " + internalId);
        	}

       		Update update = new Update();
            if (username == null) { // global metadata
                aiMap.forEach((k, v) -> update.set(GenotypingSample.SECTION_ADDITIONAL_INFO + "." + k, v));
                bulkOperations.updateMulti(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).is(internalId)), update);
            } else if (!fIsAnonymous) { // persistent user-level metadata
                aiMap.forEach((k, v) -> update.set(CustomSampleMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));
           		bulkOperations.upsert(new Query(new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_USER).is(username), new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_SAMPLE_ID).is(spId)))), update);
            } else {
            	LinkedHashMap<String, Object> existingEntityMetadata = sessionObject.get(spId);
            	if (existingEntityMetadata == null) {
            		existingEntityMetadata = new LinkedHashMap<>();
            		sessionObject.put(spId, existingEntityMetadata);
            	}
            	existingEntityMetadata.putAll(aiMap);
            }
        }

        progress.addStep("Persisting metadata found at " + endpointUrl);
        progress.moveToNextStep();

        if (!fIsAnonymous) {
            BulkWriteResult wr = bulkOperations.execute();
            return wr.getModifiedCount() + wr.getUpserts().size();
        } else {
            LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
            return 1;
        }
    }

    private static int importBrapiV2Samples(
            String sModule,
            HttpSession session,
            String endpointUrl,
            HashMap<String, String> externalToInternalIdMap,
            String username,
            String authToken,
            ProgressIndicator progress,
            MongoTemplate mongoTemplate) throws Exception {

        HashMap<String, Object> reqBody = new HashMap<>();
        reqBody.put(BRAPI_FILTER_SAMPLE_IDS, externalToInternalIdMap.keySet());

        BrapiV2Client client = new BrapiV2Client();
        client.initService(endpointUrl, authToken);
        client.getCalls();
        client.ensureSampleInfoCanBeImported();
        client.initService(endpointUrl, authToken);

        final BrapiV2Service service = client.getService();

        List<Sample> sampleList = new ArrayList<>();

        // Getting sample information by calling Brapi services
        if (client.hasCallSearchSample()) {
            progress.addStep("Getting information list from " + endpointUrl);
            progress.moveToNextStep();

            try {
                Response<SuccessfulSearchResponse> response = service.searchSamples(reqBody).execute();
                handleErrorCode(response.code());
                SuccessfulSearchResponseResult bsr = response.body().getResult();

                BrapiV2Client.Pager samplePager = new BrapiV2Client.Pager();
                while (samplePager.isPaging()) {
                    SampleListResponse br = service.searchSamplesResult(bsr.getSearchResultsDbId(), samplePager.getPageSize(), samplePager.getPage()).execute().body();
                    sampleList.addAll(br.getResult().getData());
                    samplePager.paginate(br.getMetadata());
                }
            } catch (Exception e) {    // we did not get a searchResultDbId: see if we actually got results straight away
                try {
                    Response<SampleListResponse> response = service.searchSamplesDirectResult(reqBody).execute();
                    handleErrorCode(response.code());
                    SampleListResponse br = response.body();

                    BrapiV2Client.Pager samplePager = new BrapiV2Client.Pager();
                    while (samplePager.isPaging()) {
                        sampleList.addAll(br.getResult().getData());
                        samplePager.paginate(br.getMetadata());
                    }
                } catch (Exception f) {
                    progress.setError("Error invoking BrAPI " + endpointUrl + "/search/samples call (no searchResultDbId returned and yet unable to directly obtain results)");
                    LOG.error(e);
                    LOG.error(progress.getError(), f);
                    return 0;
                }
            }
        }

        progress.addStep("Getting germplasm information from " + endpointUrl);
        progress.moveToNextStep();

        //fill map with germplasmDbIds to get linked information
//        for (Sample sample : sampleList) {
//            String sampleDbId = sample.getSampleDbId();
//            String germplasmDbId = sample.getGermplasmDbId();
//            String individual = externalToInternalIdMap.get(REF_TYPE_SAMPLE).get(sampleDbId);
//            if (externalToInternalIdMap.get(REF_TYPE_GERMPLASM) == null) {
//                externalToInternalIdMap.put(REF_TYPE_GERMPLASM, new HashMap<>());
//            }
//            externalToInternalIdMap.get(REF_TYPE_GERMPLASM).put(germplasmDbId, individual);
//        }

        //Getting information from germplasm and attributes linked to the samples
//        boolean fCanQueryAttributes = client.hasCallSearchAttributes();
//        Map<String, Map<String, Object>> germplasmMap = getBrapiV2GermplasmWithAttributes(service, endpointUrl, externalToInternalIdMap.get(REF_TYPE_GERMPLASM).keySet(), fCanQueryAttributes, progress);

        progress.addStep("Importing sample information from " + endpointUrl);
        progress.moveToNextStep();

        if (sampleList.isEmpty()) {
            return 0;
        }

        boolean fIsAnonymous = "anonymousUser".equals(username);
        LinkedHashMap<Comparable /*sample*/, LinkedHashMap<String, Object>> sessionObject = (LinkedHashMap<Comparable, LinkedHashMap<String, Object>>) session.getAttribute("samples_metadata_" + sModule);
        if (fIsAnonymous && sessionObject == null) {
            sessionObject = new LinkedHashMap<>();
            session.setAttribute("samples_metadata_" + sModule, sessionObject);
        }

        // Inserting sample information in DB as additional info
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? GenotypingSample.class : CustomSampleMetadata.class);
        for (int i = 0; i < sampleList.size(); i++) {
            Sample sample = sampleList.get(i);
            Map<String, Object> aiMap = mapper.convertValue(sample, Map.class);
            //Map<String, Object> gMap = germplasmMap.get(sample.getGermplasmDbId());
            //merging 2 maps
            //aiMap.putAll(gMap);

            progress.setCurrentStepProgress((long) (i * 100f / sampleList.size()));

            if (aiMap.isEmpty()) {
                LOG.warn("Found no metadata to import for sample " + sample.getSampleDbId());
                continue;
            }
            aiMap.remove(BrapiService.BRAPI_FIELD_sampleDbId); // we don't want to persist this field as it's internal to the remote source but not to the present system
            //aiMap.put(BrapiService.BRAPI_FIELD_extGermplasmDbId, aiMap.remove(BrapiService.BRAPI_FIELD_germplasmDbId));
            
            String internalId = externalToInternalIdMap.get(sample.getSampleDbId());
            Integer spId = null;
        	try {
        		spId = mongoTemplate.findDistinct(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).is(internalId)), "_id",  GenotypingSample.class, Integer.class).get(0);
        	}
        	catch (IndexOutOfBoundsException ioobe) {
        		throw new Exception("Unexisting sample: " + internalId);
        	}

            Update update = new Update();
            if (username == null) { // global metadata
                aiMap.forEach((k, v) -> update.set(GenotypingSample.SECTION_ADDITIONAL_INFO + "." + k, v));
                bulkOperations.updateMulti(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).is(internalId)), update);
            } else if (!fIsAnonymous) { // persistent user-level metadata
                aiMap.forEach((k, v) -> update.set(CustomSampleMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));
           		bulkOperations.upsert(new Query(new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_USER).is(username), new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_SAMPLE_ID).is(spId)))), update);
            } else {
            	LinkedHashMap<String, Object> existingEntityMetadata = sessionObject.get(spId);
            	if (existingEntityMetadata == null) {
            		existingEntityMetadata = new LinkedHashMap<>();
            		sessionObject.put(spId, existingEntityMetadata);
            	}
            	existingEntityMetadata.putAll(aiMap);
            }
        }

        progress.addStep("Persisting metadata found at " + endpointUrl);
        progress.moveToNextStep();

        if (!fIsAnonymous) {
            BulkWriteResult wr = bulkOperations.execute();
            return wr.getModifiedCount() + wr.getUpserts().size();
        }
        else {
            LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
            return 1;
        }
    }

    private static void handleErrorCode(int code) {
        if (code == 400) {
            throw new Error("HTTP request returned code 400 - Bad Request");
        }
        if (code == 401)//most probably authToken is wrong
        {
            throw new Error("HTTP request returned code 401 - Unauthorized");
        }
        if (code == 403) {
            throw new Error("HTTP request returned code 403 - Forbidden");
        }
        if (code == 404) {
            throw new Error("HTTP request returned code 404 - Not Found");
        }
        if (code == 500) {
            throw new Error("HTTP request returned code 500 - Internal Server Error");
        }
    }

    private static List<Germplasm> getBrapiV2Germplasm(BrapiV2Service service, String endPointUrl, Set<String> germplasmDbIds, ProgressIndicator progress) {
        List<Germplasm> germplasmList = new ArrayList<>();
        HashMap<String, Object> reqBody = new HashMap<>();
        reqBody.put(BRAPI_FILTER_GERMPLASM_IDS, germplasmDbIds);

        try {
            Response<SuccessfulSearchResponse> response = service.searchGermplasm(reqBody).execute();
            handleErrorCode(response.code());
            SuccessfulSearchResponseResult bsr = response.body().getResult();

            BrapiV2Client.Pager germplasmPager = new BrapiV2Client.Pager();
            while (germplasmPager.isPaging()) {
                GermplasmListResponse br = service.searchGermplasmResult(bsr.getSearchResultsDbId(), germplasmPager.getPageSize(), germplasmPager.getPage()).execute().body();
                germplasmList.addAll(br.getResult().getData());
                germplasmPager.paginate(br.getMetadata());
            }
        } catch (Exception e1) {    // we did not get a searchResultDbId: see if we actually got results straight away
            try {
                Response<GermplasmListResponse> response = service.searchGermplasmDirectResult(reqBody).execute();
                handleErrorCode(response.code());
                GermplasmListResponse br = response.body();

                BrapiV2Client.Pager germplasmPager = new BrapiV2Client.Pager();
                while (germplasmPager.isPaging()) {
                    germplasmList.addAll(br.getResult().getData());
                    germplasmPager.paginate(br.getMetadata());
                }
            } catch (Exception e2) {
                progress.setError("Error invoking BrAPI " + endPointUrl + "/search/germplasm call (no searchResultDbId returned and yet unable to directly obtain results)");
                LOG.error(e1);
                LOG.error(progress.getError(), e2);
                return new ArrayList<>();

            }
        }

        return germplasmList;
    }

    private static Map<String, List<GermplasmAttributeValue>> getBrapiV2GermplasmAttributes(BrapiV2Service service, String endPointUrl, Set<String> germplasmDbIds, ProgressIndicator progress) {

        HashMap<String, Object> reqBody = new HashMap<>();
        reqBody.put(BRAPI_FILTER_GERMPLASM_IDS, germplasmDbIds);
        List<GermplasmAttributeValue> attributesList = new ArrayList<>();

        try {
            Response<SuccessfulSearchResponse> response = service.searchAttributes(reqBody).execute();
            handleErrorCode(response.code());
            SuccessfulSearchResponseResult resultResp = response.body().getResult();

            BrapiV2Client.Pager attributesPager = new BrapiV2Client.Pager();
            while (attributesPager.isPaging()) {
                GermplasmAttributeValueListResponse attributesResp = service.searchAttributesResult(resultResp.getSearchResultsDbId(), attributesPager.getPageSize(), attributesPager.getPage()).execute().body();
                attributesList.addAll(attributesResp.getResult().getData());
                attributesPager.paginate(attributesResp.getMetadata());
            }
        } catch (Exception e1) {
            try {
                Response<GermplasmAttributeValueListResponse> response = service.searchAttributesDirectResult(reqBody).execute();
                handleErrorCode(response.code());
                GermplasmAttributeValueListResponse attributesResp = response.body();

                BrapiV2Client.Pager attributesPager = new BrapiV2Client.Pager();
                while (attributesPager.isPaging()) {
                    attributesList.addAll(attributesResp.getResult().getData());
                    attributesPager.paginate(attributesResp.getMetadata());
                }
            } catch (Exception e2) {
                progress.setError("Error invoking BrAPI " + endPointUrl + "/search/attributes call (no searchResultDbId returned and yet unable to directly obtain results)");
                LOG.error(e1);
                LOG.error(progress.getError(), e2);
                return new HashMap();

            }

        }

        // Map<germplasmDbId, List<Attributes>>
        Map<String, List<GermplasmAttributeValue>> attrMap = attributesList.stream()
                .collect(Collectors.groupingBy(attr -> attr.getGermplasmDbId(), Collectors.toList()));

        return attrMap;
    }

    private static Map<String, Map<String, Object>> getBrapiV2GermplasmWithAttributes(BrapiV2Service service, String endPointUrl, Set<String> germplasmDbIds, boolean fCanQueryAttributes, ProgressIndicator progress) throws IllegalArgumentException, IllegalAccessException {
        List<Germplasm> germplasmList = getBrapiV2Germplasm(service, endPointUrl, germplasmDbIds, progress);

        Map<String, Map<String, Object>> germplasmMap = new HashMap<>();

        if (!germplasmList.isEmpty()) {
            Map<String, List<GermplasmAttributeValue>> attributesMap = new HashMap();
            if (fCanQueryAttributes) {
                attributesMap = getBrapiV2GermplasmAttributes(service, endPointUrl, germplasmDbIds, progress);
            }            

            for (Germplasm germplasm : germplasmList) {
                Map<String, Object> aiMap = mapper.convertValue(germplasm, Map.class);
                List<GermplasmAttributeValue> attrValList = attributesMap.get(germplasm.getGermplasmDbId());
                if (attrValList != null && !attrValList.isEmpty())
                	attrValList.forEach(k -> aiMap.put(!StringUtils.isBlank(k.getAttributeName()) ? k.getAttributeName():  k.getAttributeDbId(), k.getValue()));

                germplasmMap.put(germplasm.getGermplasmDbId(), aiMap);
            }
        }

        return germplasmMap;
    }

    private static Map<String, Map<String, Object>> getBrapiV1GermplasmWithAttributes(BrapiService service, String endPointUrl, Set<String> germplasmDbIds, boolean fCanQueryAttributes, ProgressIndicator progress) throws IOException {
        HashMap<String, Object> reqBody = new HashMap<>();
        reqBody.put(BRAPI_FILTER_GERMPLASM_IDS, germplasmDbIds);
        List<BrapiGermplasm> germplasmList = new ArrayList<>();

        try {
            Response<BrapiBaseResource<BrapiSearchResult>> response = service.searchGermplasm(reqBody).execute();
            handleErrorCode(response.code());
            BrapiSearchResult bsr = response.body().getResult();

            Pager germplasmPager = new Pager();
            while (germplasmPager.isPaging()) {
                BrapiListResource<BrapiGermplasm> br = service.searchGermplasmResult(bsr.getSearchResultDbId(), germplasmPager.getPageSize(), germplasmPager.getPage()).execute().body();
                germplasmList.addAll(br.data());
                germplasmPager.paginate(br.getMetadata());
            }
        } catch (Exception e) {    // we did not get a searchResultDbId: see if we actually got results straight away
            try {
                Response<BrapiListResource<BrapiGermplasm>> response = service.searchGermplasmDirectResult(reqBody).execute();
                handleErrorCode(response.code());
                BrapiListResource<BrapiGermplasm> br = response.body();

                Pager callPager = new Pager();
                while (callPager.isPaging()) {
                    germplasmList.addAll(br.data());
                    callPager.paginate(br.getMetadata());
                }
            } catch (Exception f) {
                progress.setError("Error invoking BrAPI " + endPointUrl + "/search/germplasm call (no searchResultDbId returned and yet unable to directly obtain results)");
                LOG.error(e);
                LOG.error(progress.getError(), f);
                return new HashMap();
            }
        }   

        HashMap<String, Map<String, Object>> germplasmMap = new HashMap<>();
        for (BrapiGermplasm germplasm : germplasmList) {
            Map<String, Object> aiMap = mapper.convertValue(germplasm, Map.class);

            if (fCanQueryAttributes) {
                Response<BrapiBaseResource<BrapiGermplasmAttributes>> response = service.getAttributes(aiMap.get(BrapiService.BRAPI_FIELD_germplasmDbId).toString()).execute();
                if (response.code() != 404) {
                    handleErrorCode(response.code());
                    BrapiBaseResource<BrapiGermplasmAttributes> moreAttributes = response.body();
                    moreAttributes.getResult().getData().forEach(k -> aiMap.put(!StringUtils.isBlank(k.getAttributeName()) ? k.getAttributeName() :  k.getAttributeDbId(), k.getValue()));
                }
            }
            germplasmMap.put(germplasm.getGermplasmDbId(), aiMap);
        }
        return germplasmMap;
    }

}
