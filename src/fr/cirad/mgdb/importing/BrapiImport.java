/*******************************************************************************
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
 *******************************************************************************/
package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BasicDBObject;
import com.mongodb.client.result.DeleteResult;

import fr.cirad.io.brapi.BrapiClient;
import fr.cirad.io.brapi.BrapiClient.Pager;
import fr.cirad.io.brapi.BrapiService;
import fr.cirad.io.brapi.CallsUtils;
import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.Status;
import jhi.brapi.api.calls.BrapiCall;
import jhi.brapi.api.genomemaps.BrapiGenomeMap;
import jhi.brapi.api.genomemaps.BrapiMarkerPosition;
import jhi.brapi.api.markerprofiles.BrapiAlleleMatrix;
import jhi.brapi.api.markerprofiles.BrapiMarkerProfile;
import jhi.brapi.api.markers.BrapiMarker;
import jhi.brapi.client.AsyncChecker;
import retrofit2.Call;
import retrofit2.Response;

/**
 * The Class BrapiImport.
 */
public class BrapiImport extends AbstractGenotypeImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);

	/** The m_process id. */
	private String m_processID;
	
//	private BrapiClient client = new BrapiClient();

	private static final String unphasedGenotypeSeparator = "/"; 
	private static String phasedGenotypeSeparator = "|";
	private static String multipleGenotypeSeparatorRegex = Pattern.compile(Pattern.quote(phasedGenotypeSeparator) + "|" + Pattern.quote(unphasedGenotypeSeparator)).toString();
		
	/**
	 * Instantiates a new hap map import.
	 * @throws Exception 
	 */
	public BrapiImport() throws Exception
	{
	}

	/**
	 * Instantiates a new hap map import.
	 *
	 * @param processID the process id
	 * @throws Exception 
	 */
	public BrapiImport(String processID) throws Exception
	{
		this();
		m_processID = processID;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 7)
			throw new Exception("You must pass 7 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, ENDPOINT URL, STUDY-ID, and MAP-UD! An optional 8th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[5]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new BrapiImport().importToMongo(args[0], args[1], args[2], args[3], args[4], args[5], args[6], null, mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param endpoint URL
	 * @param studyDbId BrAPI study id
	 * @param mapDbId BrAPI map id
	 * @param brapiToken BrAPI token
	 * @param importMode the import mode
	 * @return a project ID if it was created by this method, otherwise null
	 * @throws Exception the exception
	 */
	public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, String endpointUrl, String studyDbId, String mapDbId, String brapiToken, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		final ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		
		GenericXmlApplicationContext ctx = null;
		File tempFile = File.createTempFile("brapiImportVariants-" + progress.getProcessId() + "-", ".tsv");
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
			if (m_processID == null)
				m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);

			MongoTemplateManager.lockProjectForWriting(sModule, sProject);
			
			cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

			Integer createdProject = null;
			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setTechnology(sTechnology);
				createdProject = project.getId();
			}

			BrapiClient client = new BrapiClient();	
			client.initService(endpointUrl, brapiToken);
			client.getCalls();
			client.ensureGenotypesCanBeImported();
			final BrapiService service = client.getService();

			// see if TSV format is supported by remote service
			Pager callPager = new Pager();
			List<BrapiCall> calls = new ArrayList<>();
			while (callPager.isPaging())
			{
				BrapiListResource<BrapiCall> br = service.getCalls(callPager.getPageSize(), callPager.getPage()).execute().body();
				calls.addAll(br.data());
				callPager.paginate(br.getMetadata());
			}

			boolean fMayUseTsv = client.hasAlleleMatrixSearchTSV();
//			fMayUseTsv=false;
			client.setMapID(mapDbId);
			
			Pager markerPager = new Pager();
			boolean fMayPostMarkersSearch = client.hasPostMarkersSearch(), fMayGetMarkersSearch = client.hasGetMarkersSearch(), fMayGetMarkerDetails = client.hasMarkersDetails();
			boolean fMustGuessVariantTypes = !fMayPostMarkersSearch && !fMayGetMarkersSearch && !fMayGetMarkerDetails;
			if (fMustGuessVariantTypes)
				LOG.info("Will have to guess variant types (missing calls to get this info)");

			Pager mapPager = new Pager();						
			while (mapPager.isPaging())
			{
				BrapiListResource<BrapiGenomeMap> maps = service.getMaps(null, mapPager.getPageSize(), mapPager.getPage())
						.execute()
						.body();
				for (BrapiGenomeMap map : maps.data())
				{
					if (mapDbId.equals(map.getMapDbId()))
					{
						markerPager.setPageSize("" + Math.min(map.getMarkerCount() / 10, fMustGuessVariantTypes || fMayPostMarkersSearch ? 200000 : (fMayGetMarkersSearch ? 500 : 1)));
						break;
					}
					LOG.info("Unable to determine marker count for map " + mapDbId);
				}
				mapPager.paginate(maps.getMetadata());
			}
			
			progress.addStep("Scanning existing marker IDs");
			progress.moveToNextStep();

            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true);
			
			progress.addStep("Reading remote marker list");
			progress.moveToNextStep();
			
			ArrayList<String> variantsToQueryGenotypesFor = new ArrayList<>();
			Boolean fGotKnownAllelesWhenImportingVariants = null;	// value will be determined below
			int count = 0;
			
			while (markerPager.isPaging())
			{
				LOG.debug("Querying marker page " + markerPager.getPage());
				Response<BrapiListResource<BrapiMarkerPosition>> response = service.getMapMarkerData(mapDbId, null, markerPager.getPageSize(), markerPager.getPage()).execute();
				if (!response.isSuccessful())
					throw new Exception(new String(response.errorBody().bytes()));
				BrapiListResource<BrapiMarkerPosition> positions = response.body();
		
				Map<String, VariantData> variantsToCreate = new HashMap<String, VariantData>();
				for (BrapiMarkerPosition bmp : positions.data())
				{
					variantsToQueryGenotypesFor.add(bmp.getMarkerDbId());
					if (existingVariantIDs.get(bmp.getMarkerDbId().toUpperCase()) != null)
						continue;	// we already have this one

					VariantData variant = new VariantData((ObjectId.isValid(bmp.getMarkerDbId()) ? "_" : "") + bmp.getMarkerDbId());	// prevent use of ObjectId class
					try {
					    variant.setReferencePosition(new ReferencePosition(bmp.getLinkageGroupName(), (long) Double.parseDouble(bmp.getLocation())));
					}
					catch (NumberFormatException nfe) {
					    LOG.info("No location for marker " + bmp.getMarkerDbId());
					}
					variantsToCreate.put(bmp.getMarkerDbId(), variant);
				}

				if (variantsToCreate.size() > 0)
				{	// get variant types for new variants
					Pager subPager = new Pager();
					subPager.setPageSize("" + variantsToCreate.size());

					while (!fMustGuessVariantTypes && subPager.isPaging())
					{
						Response markerReponse = null;
						if (fMayPostMarkersSearch)
						{
							HashMap<String, Object> body = new HashMap<>();
							body.put("markerDbIds", variantsToCreate.keySet());
							body.put("pageSize", Integer.parseInt(subPager.getPageSize()));
							body.put("page", Integer.parseInt(subPager.getPage()));
							markerReponse = service.getMarkerInfo_byPost(body).execute();
						}
						else if (fMayGetMarkersSearch) // try and remain compatible with older implementations of this call
							markerReponse = service.getMarkerInfo(variantsToCreate.keySet(), null, null, null, null, subPager.getPageSize(), subPager.getPage()).execute();	// try with GET
						else if (fMayGetMarkerDetails)	// worst solution: get them one by one
							markerReponse = service.getMarkerDetails(variantsToCreate.keySet().iterator().next()).execute();	// try in v1.0 mode ("markers" instead of "markers-search");
						if (!markerReponse.isSuccessful())
							throw new Exception(new String(markerReponse.errorBody().bytes()));

						BrapiListResource<BrapiMarker> markerInfo;
						if (fMayPostMarkersSearch || fMayGetMarkersSearch)
							markerInfo = ((Response<BrapiListResource<BrapiMarker>>) markerReponse).body();
						else
						{
							BrapiBaseResource<BrapiMarker> markerResource = ((Response<BrapiBaseResource<BrapiMarker>>) markerReponse).body();
							markerInfo = new BrapiListResource<BrapiMarker>(Arrays.asList(markerResource.getResult()), 0, 1, 1);
						}

						for (BrapiMarker marker : markerInfo.data())
						{
							VariantData variant = variantsToCreate.get(marker.getMarkerDbId());
							if (variant == null)
							{
								progress.setError("Marker details call returned different list from the requested one");
								return null;
							}

							if (marker.getDefaultDisplayName() != null && marker.getDefaultDisplayName().length() > 0)
							{
								TreeSet<String> internalSynonyms = variant.getSynonyms(VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL);
								if (internalSynonyms == null)
								{
									internalSynonyms = new TreeSet<>();
									variant.putSynonyms(VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL, internalSynonyms);
								}
								for (String syn : marker.getSynonyms())
									internalSynonyms.add(syn);
							}
							if (marker.getType() != null && marker.getType().length() > 0)
							{
								variant.setType(marker.getType());
								if (!project.getVariantTypes().contains(marker.getType()))
									project.getVariantTypes().add(marker.getType());
							}
							if (fGotKnownAllelesWhenImportingVariants == null)
								fGotKnownAllelesWhenImportingVariants = marker.getRefAlt() != null && marker.getRefAlt().size() > 0;
							if (fGotKnownAllelesWhenImportingVariants)
								variant.setKnownAlleles(marker.getRefAlt());
							
							// update list of existing variants (FIXME: this should be a separate method in AbstractGenotypeImport) 
							ArrayList<String> idAndSynonyms = new ArrayList<>();
							idAndSynonyms.add(variant.getId().toString());
							if (variant.getSynonyms() != null)
								for (Collection<String> syns : variant.getSynonyms().values())
									for (String syn : syns)
										idAndSynonyms.add(syn.toString());
						}
						subPager.paginate(markerInfo.getMetadata());
					}
					try
					{
					    LOG.info("Inserting " + variantsToCreate.size() + " variants from BrAPI source");
						mongoTemplate.insertAll(variantsToCreate.values());
					}
					catch (DuplicateKeyException dke)
					{
						throw new Exception("Dataset contains duplicate markers - " + dke.getMessage());
					}
					count += variantsToCreate.size();
				}
				
				if (variantsToQueryGenotypesFor.size() > variantsToCreate.size())
				{	// we already had some of them
					try
					{
						Collection<String> skippedVariants = CollectionUtils.disjunction(variantsToQueryGenotypesFor, variantsToCreate.keySet());
//						List<Comparable> fixedSkippedVariantIdList = skippedVariants.stream().map(str -> ObjectId.isValid(str) ? new ObjectId(str) : str).collect(Collectors.toList());
						project.getVariantTypes().addAll(mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).distinct(VariantData.FIELDNAME_TYPE, new Query(Criteria.where("_id").in(skippedVariants)).getQueryObject(), String.class).into(new ArrayList()));
					}
					catch (Exception e)
					{	// on big DBs querying just the ones we need leads to a query > 16 Mb
						LOG.warn("DB too big for efficiently finding distinct variant types", e);
						project.getVariantTypes().addAll(mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).distinct(VariantData.FIELDNAME_TYPE, String.class).into(new ArrayList()));
					}
				}
				
				markerPager.paginate(positions.getMetadata());
				int nCurrentPage = positions.getMetadata().getPagination().getCurrentPage();
				int nTotalPageCount = positions.getMetadata().getPagination().getTotalPages();
				progress.setCurrentStepProgress((int) ((nCurrentPage + 1) * 100f / nTotalPageCount));
			}

			progress.addStep(fMayUseTsv ? "Waiting for remote genotype file to be created" : "Downloading remote genotypes into temporary file");
			progress.moveToNextStep();
			
			client.setStudyID(studyDbId);			
			List<BrapiMarkerProfile> markerprofiles = client.getMarkerProfiles();
			HashMap<String, List<String>> germplasmToProfilesMap = new HashMap();
			for (BrapiMarkerProfile markerPofile : markerprofiles)
			{
				List<String> gpProfiles = germplasmToProfilesMap.get(markerPofile.getGermplasmDbId());
				if (gpProfiles == null)
				{
					gpProfiles = new ArrayList<String>();
					germplasmToProfilesMap.put(markerPofile.getGermplasmDbId(), gpProfiles);
				}
				gpProfiles.add(markerPofile.getMarkerprofileDbId());
			}
			HashMap<String, String> profileToGermplasmMap = new HashMap<>();
			for (String gp : germplasmToProfilesMap.keySet())
			{
				List<String> profiles = germplasmToProfilesMap.get(gp);
				if (profiles.size() > 1)
					throw new Exception("Only one markerprofile per germplasm is supported when importing a run. Found " + profiles.size() + " for " + gp);
				profileToGermplasmMap.put(profiles.get(0), gp);
			}

			LOG.debug("Importing " + markerprofiles.size() + " individuals");
			List<String> markerProfileIDs = markerprofiles.stream().map(BrapiMarkerProfile::getMarkerprofileDbId).collect(Collectors.toList());
					
			int maxPloidyFound = 0;
			LOG.debug("Importing from " + endpointUrl + " using " + (fMayUseTsv ? "TSV" : "JSON") + " format");

			final String unknownString = "";
			if (fMayUseTsv)
			{	// first call to initiate data export on server-side
				Pager genotypePager = new Pager();
				HashMap<String, Object> body = new HashMap<>();
				body.put("markerprofileDbId", markerProfileIDs);
				body.put("format", CallsUtils.TSV.get(0));
				body.put("expandHomozygotes", true);
				body.put("unknownString", unknownString);
				body.put("sepPhased", URLEncoder.encode(phasedGenotypeSeparator, "UTF-8"));
				body.put("sepUnphased", unphasedGenotypeSeparator);
				body.put("pageSize", Integer.parseInt(genotypePager.getPageSize()));
				body.put("page", Integer.parseInt(genotypePager.getPage()));
				Response<BrapiBaseResource<BrapiAlleleMatrix>> alleleMatrixResponse = service.getAlleleMatrix_byPost(body).execute();
				if (!alleleMatrixResponse.isSuccessful())
					throw new Exception("Error invoking allelematrix-search (response code " + alleleMatrixResponse.code() + ") " + new String(alleleMatrixResponse.errorBody().bytes()));
				
				BrapiBaseResource<BrapiAlleleMatrix> br = alleleMatrixResponse.body();
				List<Status> statusList = br.getMetadata().getStatus();
				String extractId = statusList != null && statusList.size() > 0 && statusList.get(0).getCode().equals("asynchid") ? statusList.get(0).getMessage() : null;
				
				while (genotypePager.isPaging())
				{
					Call<BrapiBaseResource<Object>> statusCall = service.getAlleleMatrixStatus(extractId);

					// Make an initial call to check the status on the resource
					Response<BrapiBaseResource<Object>> statusResponse = statusCall.execute();
					if (HttpServletResponse.SC_OK != statusResponse.code()) {
                        progress.setError("Wrong http code checking for allele-matrix status: " + statusResponse.code());
                        return null;
					}
					    
					BrapiBaseResource<Object> statusPoll = statusResponse.body();
					Status status = AsyncChecker.checkAsyncStatus(statusPoll.getMetadata().getStatus());

					// Keep checking until the async call returns anything else than "INPROCESS"
					while (AsyncChecker.callInProcess(status))
					{
						// Wait for a second before polling again
						try { Thread.sleep(1000); }
						catch (InterruptedException e) {}
						
						int nProgress = statusPoll.getMetadata().getPagination().getCurrentPage();
						if (nProgress != 0)
							progress.setCurrentStepProgress(nProgress);
						
						// Clone the previous retrofit call so we can call it again
						statusPoll = statusCall.clone().execute().body();
						status = AsyncChecker.checkAsyncStatus(statusPoll.getMetadata().getStatus());
					}

					if (AsyncChecker.ASYNC_FAILED.equals(status.getMessage()))
					{
						progress.setError("BrAPI export failed on server-side");
						return null;
					}
					
					if (!AsyncChecker.callFinished(status))
					{
						progress.setError("BrAPI export is in unknown status");
						return null;
					}
					else
					{
						progress.addStep("Downloading remote genotypes into temporary file");
						progress.moveToNextStep();
						
						URI uri = new URI(statusPoll.getMetadata().getDatafiles().get(0));
						HttpURLConnection httpConn = ((HttpURLConnection) uri.toURL().openConnection());
						httpConn.setInstanceFollowRedirects(true);
						
						if (Arrays.asList(HttpURLConnection.HTTP_OK, HttpURLConnection.HTTP_MOVED_PERM, HttpURLConnection.HTTP_MOVED_TEMP).contains(httpConn.getResponseCode()) && HttpURLConnection.HTTP_OK != httpConn.getResponseCode())
						{	// there's a redirection: try and handle it
							String sNewUrl = httpConn.getHeaderField("Location");
							if (sNewUrl == null || !sNewUrl.toLowerCase().startsWith("http"))
							{
								progress.setError("Unable to handle redirected URL for TSV file (http code " + httpConn.getResponseCode() + ")");
								break;
							}
							else
								httpConn = ((HttpURLConnection) new URL(sNewUrl).openConnection());
						}
				        FileUtils.copyInputStreamToFile(httpConn.getInputStream(), tempFile);

						if (existingVariantIDs.isEmpty())
							existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true);	// update it
						importTsvToMongo(sModule, project, sRun, sTechnology, tempFile.getAbsolutePath(), profileToGermplasmMap, importMode, existingVariantIDs);
						break;	// in some cases the pager keeps on paging
					}
				}
			}
			else
			{
				LOG.debug("Writing remote json data to temp file: " + tempFile);
				FileWriter tempFileWriter = new FileWriter(tempFile);
				
				int GENOTYPE_QUERY_SIZE = 30000, nChunkSize = GENOTYPE_QUERY_SIZE / markerProfileIDs.size(), nChunkIndex = 0;
		        while (nChunkIndex * nChunkSize < variantsToQueryGenotypesFor.size())
		        {
					progress.setCurrentStepProgress((nChunkIndex * nChunkSize) * 100 / variantsToQueryGenotypesFor.size());
					
			        List<String> markerSubList = variantsToQueryGenotypesFor.subList(nChunkIndex * nChunkSize, Math.min(variantsToQueryGenotypesFor.size(), ++nChunkIndex * nChunkSize));

					Pager genotypePager = new Pager();
					genotypePager.setPageSize("" + 50000);

					HashMap<String, LinkedHashSet<String>> knownAllelesByVariant = Boolean.TRUE.equals(fGotKnownAllelesWhenImportingVariants) ? null : new HashMap<>();	// if known list was determined before then we don't need to do it now
					while (genotypePager.isPaging())
					{
						BrapiBaseResource<BrapiAlleleMatrix> br = null;
						HashMap<String, Object> body = new HashMap<>();
						body.put("markerprofileDbId", markerProfileIDs);
						body.put("markerDbId", markerSubList);
						body.put("format", CallsUtils.JSON.get(0));
						body.put("expandHomozygotes", true);
						body.put("unknownString", unknownString);
						body.put("sepPhased", URLEncoder.encode(phasedGenotypeSeparator, "UTF-8"));
						body.put("sepUnphased", unphasedGenotypeSeparator);
						body.put("pageSize", Integer.parseInt(genotypePager.getPageSize()));
						body.put("page", Integer.parseInt(genotypePager.getPage()));
						Call<BrapiBaseResource<BrapiAlleleMatrix>> call = service.getAlleleMatrix_byPost(body);
						Response<BrapiBaseResource<BrapiAlleleMatrix>> response = call.execute();
						if (response.isSuccessful())
							br = response.body();
						else
							throw new Exception(new String(response.errorBody().bytes()));

						for (List<String> row : br.getResult().getData())
						{
							String genotype = row.get(2);
							if (unknownString.equals(genotype) || "N".equals(genotype))
							{
//								System.out.print(row.get(0) + " ");
								continue;
							}

							String[] alleles = genotype.split(multipleGenotypeSeparatorRegex);
							if (knownAllelesByVariant != null)
							{
								LinkedHashSet<String> variantAlleles = knownAllelesByVariant.get(row.get(0));
								if (variantAlleles == null)
								{
									variantAlleles = new LinkedHashSet<>();
									knownAllelesByVariant.put(row.get(0), variantAlleles);
								}
								variantAlleles.addAll(Arrays.asList(alleles));
							}

							int ploidy = alleles.length;
							if (maxPloidyFound < ploidy)
								maxPloidyFound = ploidy;
							tempFileWriter.write(". " + profileToGermplasmMap.get(row.get(1)) + " " + row.get(0) + " " + genotype.replaceAll(multipleGenotypeSeparatorRegex, " ") + "\n");
						}
						
						if (knownAllelesByVariant != null) {
							// build reverse hashmap for faster db update
							HashMap<String, List<String>> variantsByKnownAlleles = new HashMap<>();
							for (String variant : knownAllelesByVariant.keySet())
							{
								String knownAlleleCsv = StringUtils.join(knownAllelesByVariant.get(variant), ",");
								List<String> variantForAlleles = variantsByKnownAlleles.get(knownAlleleCsv);
								if (variantForAlleles == null)
								{
									variantForAlleles = new ArrayList<>();
									variantsByKnownAlleles.put(knownAlleleCsv, variantForAlleles);
								}
								variantForAlleles.add(variant);
							}
	
							for (String knownAlleleCsv : variantsByKnownAlleles.keySet())
							{
								List<String> variants = variantsByKnownAlleles.get(knownAlleleCsv);
								Query q = new Query(Criteria.where("_id").in(variants));
								if (existingVariantIDs.isEmpty())
									mongoTemplate.updateMulti(q, new Update().set(VariantData.FIELDNAME_KNOWN_ALLELES, Helper.split(knownAlleleCsv, ",")), VariantData.class);
								else	// we need to be more careful and avoid to delete existing known alleles
									for (String allele : Helper.split(knownAlleleCsv, ","))
										mongoTemplate.updateMulti(q, new Update().addToSet(VariantData.FIELDNAME_KNOWN_ALLELES, allele), VariantData.class);
							}
						}
						
						genotypePager.paginate(br.getMetadata());
						tempFileWriter.flush();				
					}
		        }
				tempFileWriter.close();

		        // STDVariantImport is convenient because it always sorts data by variants
				STDVariantImport stdVariantImport = new STDVariantImport(progress.getProcessId());
				mongoTemplate.save(project);	// save the project so it can be re-opened by our STDVariantImport
				stdVariantImport.setPloidy(maxPloidyFound);
				stdVariantImport.allowDbDropIfNoGenotypingData(false);
				stdVariantImport.tryAndMatchRandomObjectIDs(true);
				stdVariantImport.importToMongo(sModule, sProject, sRun, sTechnology, tempFile.getAbsolutePath(), importMode);
			}
			
	        // last, remove any variants that have no associated alleles
	        DeleteResult dr = mongoTemplate.remove(new Query(Criteria.where(VariantData.FIELDNAME_KNOWN_ALLELES + ".0").exists(false)), VariantData.class);
	        if (dr.getDeletedCount() > 0)
	        {
	        	LOG.debug("Removed " + dr.getDeletedCount() + " variants without known alleles");
	        	count -= dr.getDeletedCount();
	        	if (count == 0)
	        		LOG.error("Unable to get alleles for this dataset's variants (database " + sModule + ")");
	        }

			LOG.info("BrapiImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
			return createdProject;
		}
		catch (SocketException se)
		{
			if ("Connection reset".equals(se.getMessage()))
			{
				LOG.error("Error invoking BrAPI service. Try and check server-side logs", se);
				throw new Exception("Error invoking BrAPI service", se);
			}
			throw se;
		}
		finally
		{
			if (tempFile.exists())
			{
//				System.out.println("temp file size: " + tempFile.length());
				tempFile.delete();
			}
			if (ctx != null)
				ctx.close();
			
			MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
		}
	}
	
	public void importTsvToMongo(String sModule, GenotypingProject project, String sRun, String sTechnology, String mainFilePath, Map<String, String> markerProfileToIndividualMap, int importMode, HashMap<String, String> existingVariantIDs) throws Exception
	{
		long before = System.currentTimeMillis();
		ProgressIndicator progress = ProgressIndicator.get(m_processID);
		if (progress == null)
			progress = new ProgressIndicator(m_processID, new String[] {"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		ProgressIndicator.registerProgressIndicator(progress);
		
		GenericXmlApplicationContext ctx = null;
		File genotypeFile = new File(mainFilePath);
		BufferedReader in = new BufferedReader(new FileReader(genotypeFile));
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(sModule);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + sModule + "' does not exist!");
			}
			
			mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));	// disable profiling
			
			// Find out ploidy level
			in.readLine();	// skip header line
			String sLine = in.readLine();
			int nResolvedPloidy = 0;
			long lineCount = 0;
			while (sLine != null && lineCount++ < 1000)
			{
				if (sLine.length() > 0)
				{
					List<String> splitLine = Helper.split(sLine.trim(), "\t");
					try
					{
						Integer maxPloidyForVariant = splitLine.subList(1, splitLine.size()).stream().map(gt -> gt.split("\\||\\/").length).reduce(Integer::max).get();
						if (maxPloidyForVariant > nResolvedPloidy)
							nResolvedPloidy = maxPloidyForVariant;
					}
					catch (NoSuchElementException ignored)
					{}
				}
				sLine = in.readLine();
			}

			if (importMode == 0 && project.getPloidyLevel() != 0 && project.getPloidyLevel() != nResolvedPloidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nResolvedPloidy + ") data!");
			project.setPloidyLevel(nResolvedPloidy);
			LOG.info("Using max ploidy found among first 1000 variants: " + nResolvedPloidy);
			
			in.close();
			in = new BufferedReader(new FileReader(genotypeFile));

			// The first line is a list of marker profile IDs
			List<String> individuals = Arrays.asList(in.readLine().split("\t"));
			individuals = individuals.subList(1, individuals.size());

			// import genotyping data
			progress.addStep("Processing variant lines");
			progress.moveToNextStep();
			progress.setPercentageEnabled(false);		
			sLine = in.readLine();
			int nVariantSaveCount = 0;
			lineCount = 0;
			String sVariantName = null;
			ArrayList<String> unsavedVariants = new ArrayList<String>();
			Map<String, GenotypingSample> previouslyCreatedSamples = new TreeMap<>();	// will auto-magically remove all duplicates, and sort data, cool eh?
			TreeSet<String> affectedSequences = new TreeSet<String>();	// will contain all sequences containing variants for which we are going to add genotypes
			HashMap<String /*individual*/, String> phasingGroup = new HashMap<>();
			do
			{
				if (sLine.length() > 0)
				{
					List<String> splitLine = Helper.split(sLine.trim(), "\t");
					String[] cells = splitLine.toArray(new String[splitLine.size()]);
					sVariantName = (ObjectId.isValid(cells[0]) ? "_" : "") + cells[0];	// prevent use of ObjectId class
					String mgdbVariantId = existingVariantIDs.get(sVariantName.toUpperCase());
					if (mgdbVariantId == null)
						LOG.warn("Unknown id: " + sVariantName);
					else if (mgdbVariantId.toString().startsWith("*"))
						LOG.warn("Skipping deprecated variant data: " + sVariantName);
					else if (saveWithOptimisticLock(mongoTemplate, project, sRun, individuals, markerProfileToIndividualMap, mgdbVariantId, new HashMap<String, ArrayList<String>>() /*FIXME or ditch me*/, sLine, 3, previouslyCreatedSamples, affectedSequences, phasingGroup))
						nVariantSaveCount++;
					else
						unsavedVariants.add(sVariantName);
				}
				sLine = in.readLine();
				progress.setCurrentStepProgress((int) ++lineCount);
			}
			while (sLine != null);
			
			if (nVariantSaveCount == 0)
				throw new Exception("No variation data could be imported. Please check the logs.");

            project.getSequences().addAll(affectedSequences);
			
			// save project data
            if (!project.getRuns().contains(sRun)) {
                project.getRuns().add(sRun);
            }
			mongoTemplate.save(project);	// always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
			mongoTemplate.insert(previouslyCreatedSamples.values(), GenotypingSample.class);
	
	    	LOG.info("Import took " + (System.currentTimeMillis() - before)/1000 + "s for " + lineCount + " CSV lines (" + nVariantSaveCount + " variants were saved)");
	    	if (unsavedVariants.size() > 0)
	    	   	LOG.warn("The following variants could not be saved because of concurrent writing: " + StringUtils.join(unsavedVariants, ", "));
	    	
			progress.addStep("Preparing database for searches");
			progress.moveToNextStep();
			MgdbDao.prepareDatabaseForSearches(mongoTemplate);
			progress.markAsComplete();
		}
		finally
		{
			if (in != null)
				in.close();
			if (ctx != null)
				ctx.close();
		}
	}
	
	private static boolean saveWithOptimisticLock(MongoTemplate mongoTemplate, GenotypingProject project, String runName, List<String> markerProfiles, Map<String, String> markerProfileToIndividualMap, String mgdbVariantId, HashMap<String, ArrayList<String>> inconsistencies, String lineForVariant, int nNumberOfRetries, Map<String, GenotypingSample> usedSamples, TreeSet<String> affectedSequences, HashMap<String /*individual*/, String> phasingGroup) throws Exception
	{		
		for (int j=0; j<Math.max(1, nNumberOfRetries); j++)
		{			
			Query query = new Query(Criteria.where("_id").is(mgdbVariantId));
			query.fields().include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_KNOWN_ALLELES).include(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId()).include(VariantData.FIELDNAME_VERSION);
			
			VariantData variant = mongoTemplate.findOne(query, VariantData.class);
			Update update = variant == null ? null : new Update();
			ReferencePosition rp = variant.getReferencePosition();
			if (rp != null)
				affectedSequences.add(rp.getSequence());
			
			String sVariantName = lineForVariant.trim().split("\t")[0];
			
			VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, mgdbVariantId));
			
			ArrayList<String> inconsistentIndividuals = inconsistencies.get(mgdbVariantId);
			String[] cells = lineForVariant.trim().split("\t");
			boolean fNewAllelesEncountered = false;
			for (int k=1; k<=markerProfiles.size(); k++)
			{				
				String sIndividual = markerProfileToIndividualMap.get(markerProfiles.get(k - 1));

				if (!usedSamples.containsKey(sIndividual))	// we don't want to persist each sample several times
				{
	                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
	                if (ind == null) {	// we don't have any population data so we don't need to update the Individual if it already exists
	                    ind = new Individual(sIndividual);
	                    mongoTemplate.save(ind);
	                }

	                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
	                usedSamples.put(sIndividual, new GenotypingSample(sampleId, project.getId(), vrd.getRunName(), sIndividual));	// add a sample for this individual to the project
	            }

				String gtString = "";
				boolean fInconsistentData = inconsistentIndividuals != null && inconsistentIndividuals.contains(sIndividual);
				if (fInconsistentData)
					LOG.warn("Not adding inconsistent data: " + sVariantName + " / " + sIndividual);
				else
				{					
					ArrayList<Integer> alleleIndexList = new ArrayList<Integer>();
					String phasedGT = null;
					if (k < cells.length && cells[k].length() > 0/* && !"N".equals(cells[k])*/)
					{
						if (cells[k].contains("|"))
							phasedGT = cells[k];
						
						String phasedGroup = phasingGroup.get(sIndividual);
			            if (phasedGroup == null || (phasedGT == null))
			                phasingGroup.put(sIndividual, variant.getId());

						String[] alleles = cells[k].split(multipleGenotypeSeparatorRegex);
						if (alleles.length != project.getPloidyLevel() && alleles.length > 1)
							LOG.warn("Not adding genotype " + cells[k] + " because it doesn't match ploidy level (" + project.getPloidyLevel() + "): " + sVariantName + " / " + sIndividual);
						else
							for (int i=0; i<project.getPloidyLevel(); i++)
							{
								int indexToUse = alleles.length == project.getPloidyLevel() ? i : 0;	// support for collapsed homozygous genotypes
								if (!variant.getKnownAlleles().contains(alleles[indexToUse]))
								{
									variant.getKnownAlleles().add(alleles[indexToUse]);	// it's the first time we encounter this alternate allele for this variant
									fNewAllelesEncountered = true;
								}
								alleleIndexList.add(variant.getKnownAlleles().indexOf(alleles[indexToUse]));
							}
					}
					Collections.sort(alleleIndexList);
					gtString = StringUtils.join(alleleIndexList, "/");
					if (gtString.equals(""))
						continue;

					SampleGenotype genotype = new SampleGenotype(gtString);
					vrd.getSampleGenotypes().put(usedSamples.get(sIndividual).getId(), genotype);
		            if (phasedGT != null) {
		            	genotype.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_GT, StringUtils.join(alleleIndexList, "|"));
		            	genotype.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_ID, phasingGroup.get(sIndividual));
		            }
				}
			}
			if (fNewAllelesEncountered && update != null)
				update.set(VariantData.FIELDNAME_KNOWN_ALLELES, variant.getKnownAlleles());
			if (variant.getType() == null && update != null)
			{	// no variant type was explicitly specified, so try and determine it
				variant.setType(VariantData.determinePolymorphicType(variant.getKnownAlleles()).toString());
				update.set(VariantData.FIELDNAME_TYPE, variant.getType());
				project.getVariantTypes().add(variant.getType());
			}
			project.getAlleleCounts().add(variant.getKnownAlleles().size());	// it's a TreeSet so it will only be added if it's not already present

			try
			{
				if (!update.getUpdateObject().keySet().isEmpty())
				{
					mongoTemplate.upsert(new Query(Criteria.where("_id").is(mgdbVariantId)).addCriteria(Criteria.where(VariantData.FIELDNAME_VERSION).is(variant.getVersion())), update, VariantData.class);
				}
		        vrd.setKnownAlleles(variant.getKnownAlleles());
		        vrd.setReferencePosition(variant.getReferencePosition());
		        vrd.setType(variant.getType());
		        vrd.setSynonyms(variant.getSynonyms());
				mongoTemplate.save(vrd);

				if (j > 0)
					LOG.info("It took " + j + " retries to save variant " + variant.getId());
				return true;
			}
			catch (OptimisticLockingFailureException olfe)
			{
//				LOG.info("failed: " + variant.getId());
			}
		}
		return false;	// all attempts failed
	}
}