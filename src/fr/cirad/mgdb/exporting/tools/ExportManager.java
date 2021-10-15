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
package fr.cirad.mgdb.exporting.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.IntKeyMapPropertyCodecProvider;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import fr.cirad.mgdb.exporting.AbstractExportWritingThread;
import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The class ExportManager.
 */
public class ExportManager
{
	/** The Constant LOG. */
	static final Logger LOG = Logger.getLogger(ExportManager.class);
	
	static public final AlphaNumericComparator<String> alphaNumericStringComparator = new AlphaNumericComparator<String>();
	
	static public final class VariantRunDataComparator implements Comparator<VariantRunData> {
	    private int nAssemblyId;
	    
	    public VariantRunDataComparator(int nAssemblyId) {
	        this.nAssemblyId = nAssemblyId;
	    }
	    
		@Override
		public int compare(VariantRunData vrd1, VariantRunData vrd2) {
			if (vrd1.getReferencePosition(nAssemblyId) == null) {
				if (vrd2.getReferencePosition(nAssemblyId) == null)
					return vrd1.getVariantId().compareTo(vrd2.getVariantId()); 	// none is positioned
				return -1;	// only vrd2 is positioned
			}
			if (vrd2.getReferencePosition(nAssemblyId) == null)
				return 1;	// only vrd1 is positioned
			
			// both are positioned
			int chrComparison = alphaNumericStringComparator.compare(vrd1.getReferencePosition(nAssemblyId).getSequence(), vrd2.getReferencePosition(nAssemblyId).getSequence());
			return chrComparison != 0 ? chrComparison : (int) (vrd1.getReferencePosition(nAssemblyId).getStartSite() - vrd2.getReferencePosition(nAssemblyId).getStartSite());
		}
	}
	
	@SuppressWarnings("rawtypes")
	private MongoCursor markerCursor;
	
	private ProgressIndicator progress;
	
	private int nQueryChunkSize;
	
	private boolean fWorkingOnTempColl;
	
	private FileWriter warningFileWriter;
	
	private List<BasicDBObject> pipeline = new ArrayList<>();
	
	private MongoTemplate mongoTemplate;
	
	private AbstractExportWritingThread writingThread;
	
	private Long markerCount;
	
	private Collection<Integer> sampleIDsToExport;
	
	private ArrayList<Document> projectFilterList = new ArrayList<>();
	
	private int nAssemblyId;

	public static final CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().register(new IntKeyMapPropertyCodecProvider()).automatic(true).build()));
	
	public ExportManager(MongoTemplate mongoTemplate, int nAssemblyId, MongoCollection<Document> varColl, Class resultType, Document varQuery, Collection<GenotypingSample> samplesToExport, boolean fIncludeMetadata, int nQueryChunkSize, AbstractExportWritingThread writingThread, Long markerCount, FileWriter warningFileWriter, ProgressIndicator progress) {
		this.progress = progress;
		this.nQueryChunkSize = nQueryChunkSize;
		this.warningFileWriter = warningFileWriter;
		this.mongoTemplate = mongoTemplate;
		this.nAssemblyId = nAssemblyId;
		this.writingThread = writingThread;
		this.markerCount = markerCount;

		String varCollName = varColl.getNamespace().getCollectionName();
		fWorkingOnTempColl = varCollName.startsWith(MongoTemplateManager.TEMP_COLL_PREFIX);

		// optimization 1: filling in involvedProjectRuns will provide means to apply filtering on project and/or run fields when exporting from temporary collection
		HashMap<Integer, List<String>> involvedProjectRuns = Helper.getRunsByProjectInSampleCollection(samplesToExport);
		int nDbProjectCount = (int) Helper.estimDocCount(mongoTemplate, GenotypingProject.class);
		for (int projId : involvedProjectRuns.keySet()) {
			List<String> projectInvolvedRuns = involvedProjectRuns.get(projId);
			if (projectInvolvedRuns.size() != mongoTemplate.findDistinct(new Query(Criteria.where("_id").is(projId)), GenotypingProject.FIELDNAME_RUNS, GenotypingProject.class, String.class).size()) {
				// not all of this project's runs are involved: only match those (in the case of multi-project and/or multi-run databases, it is worth filtering on these fields to avoid parsing records related to unwanted samples)
				projectFilterList.add(new Document("$and", Arrays.asList(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, projId), new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_RUNNAME, new BasicDBObject("$in", projectInvolvedRuns)))));
			}
			else if (nDbProjectCount != involvedProjectRuns.size())
				projectFilterList.add(new Document("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, projId));	// not all of this DB's projects are involved: only match selected ones
		}

		// optimization 2: build the $project stage by excluding fields when over 50% of overall samples are selected; even remove that stage when exporting all (or almost all) samples (makes it much faster)
		long percentageOfExportedSamples = 100 * samplesToExport.size() / Helper.estimDocCount(mongoTemplate, GenotypingSample.class);
		sampleIDsToExport = samplesToExport == null ? new ArrayList() : samplesToExport.stream().map(sp -> sp.getId()).collect(Collectors.toList());
		Collection<Integer>	sampleIDsNotToExport = percentageOfExportedSamples >= 98 ? new ArrayList() /* if almost all individuals are being exported we directly omit the $project stage */ : (percentageOfExportedSamples > 50 ? mongoTemplate.findDistinct(new Query(Criteria.where("_id").not().in(sampleIDsToExport)), "_id", GenotypingSample.class, Integer.class) : null);

		if (!varQuery.isEmpty()) {
			if (!projectFilterList.isEmpty()) {
				Entry<String, Object> firstMatchEntry = varQuery.entrySet().iterator().next();
				List<Document> matchAndList = "$and".equals(firstMatchEntry.getKey()) ? (List<Document>) firstMatchEntry.getValue() : Arrays.asList(varQuery);
				System.err.println(matchAndList);
				matchAndList.addAll(projectFilterList);
			}
			pipeline.add(new BasicDBObject("$match", varQuery));
		}

		String refPosPath = AbstractVariantData.FIELDNAME_REFERENCE_POSITION;
		BasicDBObject sortStage = new BasicDBObject("$sort", new Document(refPosPath  + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(refPosPath + "." + ReferencePosition.FIELDNAME_START_SITE, 1));
		pipeline.add(sortStage);

		Document projection = new Document();
		if (sampleIDsNotToExport == null) {	// inclusive $project (less than a half of the samples are being exported)
			projection.append(refPosPath, 1);
			projection.append(AbstractVariantData.FIELDNAME_KNOWN_ALLELE_LIST, 1);
			projection.append(AbstractVariantData.FIELDNAME_TYPE, 1);
			projection.append(AbstractVariantData.FIELDNAME_SYNONYMS, 1);
			projection.append(AbstractVariantData.FIELDNAME_ANALYSIS_METHODS, 1);
			if (fIncludeMetadata)
				projection.append(AbstractVariantData.SECTION_ADDITIONAL_INFO, 1);
		}

		for (Integer spId : sampleIDsNotToExport == null ? sampleIDsToExport : sampleIDsNotToExport)
			projection.append(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + spId, sampleIDsNotToExport == null ? 1 /*include*/ : 0 /*exclude*/);

		if (!projection.isEmpty()) {
			if (sampleIDsNotToExport != null && !fIncludeMetadata)	// exclusive $project (more than a half of the samples are being exported). We only add it if the $project stage is already justified (it's expensive so avoiding it is better when possible)
				projection.append(AbstractVariantData.SECTION_ADDITIONAL_INFO, 0);

			pipeline.add(new BasicDBObject("$project", projection));
		}

		LOG.debug("Export pipeline: " + pipeline);
		
//    	long before = System.currentTimeMillis();
    	markerCursor = varColl.aggregate(fWorkingOnTempColl ? Arrays.asList(sortStage, new BasicDBObject("$project", new BasicDBObject("_id", 1))) : pipeline, fWorkingOnTempColl ? Document.class : resultType).collation(IExportHandler.collationObj).allowDiskUse(true).batchSize(nQueryChunkSize).iterator();	/*FIXME: didn't find a way to set noCursorTimeOut on aggregation cursors*/
//		System.err.println("cursor obtained in " + (System.currentTimeMillis() - before) + "ms");
	}

    public void readAndWrite() throws IOException, InterruptedException, ExecutionException {		
		if (fWorkingOnTempColl)
			exportFromTempColl(nAssemblyId);
		else
			exportDirectlyFromRuns(nAssemblyId);
    }
    
    /**
     * Exports by $match-ing successive chunks of variant IDs in VariantRunData. Would have thought using $lookup with a single cursor would be faster, but it's much slower
     * 
     * @param nAssemblyId the assembly id
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
	private void exportFromTempColl(int nAssemblyId) throws IOException, InterruptedException, ExecutionException {
		CompletableFuture<Void> future = null;
		List<List<VariantRunData>> tempMarkerRunsToWrite = new ArrayList<>(nQueryChunkSize);
		List<VariantRunData> currentMarkerRuns = new ArrayList<>();
		List<String> currentMarkerIDs = new ArrayList<>();
		String varId = null, previousVarId = null;
		int nWrittenmarkerCount = 0;
		
		VariantRunDataComparator vrdComparator = new VariantRunDataComparator(nAssemblyId);
		
		MongoCollection<VariantRunData> runColl = mongoTemplate.getDb().withCodecRegistry(ExportManager.pojoCodecRegistry).getCollection(mongoTemplate.getCollectionName(VariantRunData.class), VariantRunData.class);
		while (markerCursor.hasNext()) {
            if (progress.isAborted() || progress.getError() != null ) {
            	if (warningFileWriter != null)
            		warningFileWriter.close();
			    return;
            }
            
			currentMarkerIDs.add(((Document) markerCursor.next()).getString("_id"));
			
			if (currentMarkerIDs.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
				BasicDBList matchAndList = new BasicDBList();
		        if (!projectFilterList.isEmpty())
		        	matchAndList.add(projectFilterList.size() == 1 ? projectFilterList.get(0) : new BasicDBObject("$or", projectFilterList));
				matchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$in", currentMarkerIDs)));

				BasicDBObject initialMatchStage = new BasicDBObject("$match", new BasicDBObject("$and", matchAndList));
				if (varId == null)
					pipeline.add(0, initialMatchStage);	// first time we set this stage, it's an insertion
				else
					pipeline.set(0, initialMatchStage);	// replace existing $match

				ArrayList<VariantRunData> runs = runColl.aggregate(pipeline, VariantRunData.class).allowDiskUse(true).into(new ArrayList<>()); // we don't use collation here because it leads to unexpected behaviour (sometimes fetches some additional variants to those in currentMarkerIDs) => we'll have to sort each chunk by hand
				Collections.sort(runs, vrdComparator);	// make sure variants within this chunk are correctly sorted
				
				for (VariantRunData vrd : runs) {
					varId = vrd.getId().getVariantId();
					
					if (previousVarId != null && !varId.equals(previousVarId)) {
						tempMarkerRunsToWrite.add(currentMarkerRuns);
						currentMarkerRuns = new ArrayList<>();
						nWrittenmarkerCount++;
					}
					currentMarkerRuns.add(vrd);
					previousVarId = varId;
				}
				
				if (!markerCursor.hasNext())
					tempMarkerRunsToWrite.add(currentMarkerRuns);	// special case, when the end of the cursor is being reached
				currentMarkerIDs.clear();

				if (future != null && !future.isDone()) {
//					long b4 = System.currentTimeMillis();
					future.get();
//					long delay = System.currentTimeMillis() - b4;
//					if (delay > 100)
//						LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenmarkerCount/* + ", increasing nQueryChunkSize from " + nQueryChunkSize + " to " + nQueryChunkSize*2*/);
				}
				
				if (markerCount != null)
					progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
				future = writingThread.writeRuns(tempMarkerRunsToWrite);
				tempMarkerRunsToWrite.clear();
			}
		}

		if (future != null && !future.isDone())
			future.get();
		if (markerCount != null)
			progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
	}

    /**
     * Exports by looping on existing cursor
     * 
     * @param nAssemblyId the assembly id
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
	private void exportDirectlyFromRuns(int nAssemblyId) throws IOException, InterruptedException, ExecutionException {
		CompletableFuture<Void> future = null;
		List<List<VariantRunData>> tempMarkerRunsToWrite = new ArrayList<>(nQueryChunkSize);
		List<VariantRunData> currentMarkerRuns = new ArrayList<>();
		String varId = null, previousVarId = null;
		int nWrittenmarkerCount = 0;
		
		while (markerCursor.hasNext()) {
            if (progress.isAborted() || progress.getError() != null ) {
            	if (warningFileWriter != null)
            		warningFileWriter.close();
			    return;
            }
            
			VariantRunData vrd = (VariantRunData) markerCursor.next();
			varId = vrd.getId().getVariantId();

			if (previousVarId != null && !varId.equals(previousVarId)) {
				tempMarkerRunsToWrite.add(currentMarkerRuns);
				currentMarkerRuns = new ArrayList<>();
				nWrittenmarkerCount++;
			}

			currentMarkerRuns.add(vrd);

			if (!markerCursor.hasNext())
				tempMarkerRunsToWrite.add(currentMarkerRuns);	// special case, when the end of the cursor is being reached

			if (tempMarkerRunsToWrite.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
				if (future != null && !future.isDone()) {
//					long b4 = System.currentTimeMillis();
					future.get();
//					long delay = System.currentTimeMillis() - b4;
//					if (delay > 100) {
//						LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenmarkerCount/* + ", increasing nQueryChunkSize from " + nQueryChunkSize + " to " + nQueryChunkSize*2*/);
				}

				if (markerCount != null && markerCount > 0)
					progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
				future = writingThread.writeRuns(tempMarkerRunsToWrite);
				tempMarkerRunsToWrite.clear();
			}
			previousVarId = varId;
		}

		if (future != null && !future.isDone())
			future.get();
		if (markerCount != null && markerCount > 0)
			progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
	}
}