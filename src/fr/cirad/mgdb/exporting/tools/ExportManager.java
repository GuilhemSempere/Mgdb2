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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import fr.cirad.mgdb.model.mongo.maintypes.*;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
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
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import fr.cirad.tools.query.GroupedExecutor;
import fr.cirad.tools.query.GroupedExecutor.TaskWrapper;

import javax.ejb.ObjectNotFoundException;

/**
 * The class ExportManager.
 */
public class ExportManager
{
    
    /** The Constant LOG. */
    static final Logger LOG = Logger.getLogger(ExportManager.class);
    
    static public final AlphaNumericComparator<String> alphaNumericStringComparator = new AlphaNumericComparator<String>();
    
    static public final class VariantRunDataComparator implements Comparator<VariantRunData> {
        private Integer nAssemblyId;
        
        public VariantRunDataComparator(Integer nAssemblyId) {
            this.nAssemblyId = nAssemblyId;
        }
        
        @Override
        public int compare(VariantRunData vrd1, VariantRunData vrd2) {
            if (vrd1.getReferencePosition(nAssemblyId) == null) {
                if (vrd2.getReferencePosition(nAssemblyId) == null)
                    return vrd1.getVariantId().compareTo(vrd2.getVariantId());  // none is positioned
                return -1;  // only vrd2 is positioned
            }
            if (vrd2.getReferencePosition(nAssemblyId) == null)
                return 1;   // only vrd1 is positioned
            
            // both are positioned
            int chrComparison = alphaNumericStringComparator.compare(vrd1.getReferencePosition(nAssemblyId).getSequence(), vrd2.getReferencePosition(nAssemblyId).getSequence());
            return chrComparison != 0 ? chrComparison : (int) (vrd1.getReferencePosition(nAssemblyId).getStartSite() - vrd2.getReferencePosition(nAssemblyId).getStartSite());
        }
    }

    private ProgressIndicator progress;
    
    private int nQueryChunkSize;
    
    private boolean fWorkingOnTempColl;
    
    private String module;
    
    private AbstractExportWriter exportWriter;
    
    private Long markerCount;
    
    private Collection<String> sampleIDsToExport;
    
    private MongoCollection<Document> varColl;
    
    private Class resultType;
    
    int involvedRunCount;
    
    private BasicDBObject variantMatchStage = null;
    private BasicDBObject sortStage = null;
    private BasicDBObject projectStage = null;
    
    private Integer nNumberOfChunksUsedForSpeedEstimation = null;  // if it remains null then we won't attempt any comparison
    
    private ArrayList<BasicDBObject> projectFilterList = new ArrayList<>();
    
    private Integer nAssemblyId;
    
    private File[] chunkGenotypeFiles;
    private File[] chunkVariantFiles;
    private File[] chunkWarningFiles;
    
	private File dataExtractionFolder;

    public static final CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().register(new IntKeyMapPropertyCodecProvider()).automatic(true).build()));

	public ExportManager(String sModule, Integer nAssemblyId, MongoCollection<Document> varColl, Class resultType, Document variantQuery, Collection<GenotypingSample> samplesToExport, boolean fIncludeMetadata, int nQueryChunkSize, AbstractExportWriter exportWriter, Long markerCount, ProgressIndicator progress) throws ObjectNotFoundException {
        this.progress = progress;
        this.nQueryChunkSize = nQueryChunkSize;
        this.module = sModule;
        this.nAssemblyId = nAssemblyId;
        this.exportWriter = exportWriter;
        this.markerCount = markerCount;
        this.varColl = varColl;
        this.resultType = resultType;

        String varCollName = varColl.getNamespace().getCollectionName();
        fWorkingOnTempColl = varCollName.startsWith(MongoTemplateManager.TEMP_COLL_PREFIX);

        String refPosPath = Assembly.getVariantRefPosPath(nAssemblyId);
        sortStage = new BasicDBObject("$sort", new Document(refPosPath  + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(refPosPath + "." + ReferencePosition.FIELDNAME_START_SITE, 1));

        // optimization 1: filling in involvedProjectRuns will provide means to apply filtering on project and/or run fields when exporting from temporary collection
        List<CallSet> callsets = samplesToExport.stream().map(sp -> sp.getCallSets()).flatMap(Collection::stream).toList();
        HashMap<Integer, List<String>> involvedProjectRuns = Helper.getRunsByProjectInCallsetCollection(callsets);
        involvedRunCount = involvedProjectRuns.values().stream().mapToInt(b -> b.size()).sum();

        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);

        boolean fNotAllProjectsNeeded = Helper.estimDocCount(mongoTemplate, GenotypingProject.class) > involvedProjectRuns.size();	// if it's a multi-project DB we'd better filter on project field (less records treated, faster export)
        for (int projId : involvedProjectRuns.keySet()) {
            List<String> projectInvolvedRuns = involvedProjectRuns.get(projId);
            BasicDBObject projectFilter = new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, projId);
            // if not all of this project's runs are involved, only match the required ones
            boolean fNotAllRunsNeeded = projectInvolvedRuns.size() != mongoTemplate.findDistinct(new Query(Criteria.where("_id").is(projId)), GenotypingProject.FIELDNAME_RUNS, GenotypingProject.class, String.class).size();
            if (fNotAllProjectsNeeded || fNotAllRunsNeeded)
                projectFilterList.add(fNotAllRunsNeeded ? new BasicDBObject("$and", Arrays.asList(projectFilter, new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_RUNNAME, new BasicDBObject("$in", projectInvolvedRuns)))) : projectFilter);
        }

        // optimization 2: build the $project stage by excluding fields when over 50% of overall samples are selected; even remove that stage when exporting all (or almost all) samples (makes it much faster)
        long nTotalNumberOfSamplesInDB = Helper.estimDocCount(mongoTemplate, GenotypingSample.class);
        long percentageOfExportedSamples = nTotalNumberOfSamplesInDB == 0 ? 100 : 100 * samplesToExport.size() / nTotalNumberOfSamplesInDB;
        sampleIDsToExport = samplesToExport == null ? new ArrayList<>() : samplesToExport.stream().map(sp -> sp.getId()).collect(Collectors.toList());
        Collection<String> sampleIDsNotToExport = percentageOfExportedSamples >= 98 ? new ArrayList<>() /* if almost all individuals are being exported we directly omit the $project stage */ : (percentageOfExportedSamples > 50 ? mongoTemplate.findDistinct(new Query(Criteria.where("_id").not().in(sampleIDsToExport)), "_id", GenotypingSample.class, String.class) : null);

        if (variantQuery != null && !variantQuery.isEmpty())
            variantMatchStage = new BasicDBObject("$match", variantQuery);

        Document projection = new Document();
        if (sampleIDsNotToExport == null) {    // inclusive $project (less than a half of the samples are being exported)
            projection.append(refPosPath, 1);
            projection.append(AbstractVariantData.FIELDNAME_KNOWN_ALLELES, 1);
            projection.append(AbstractVariantData.FIELDNAME_TYPE, 1);
            projection.append(AbstractVariantData.FIELDNAME_SYNONYMS, 1);
            projection.append(AbstractVariantData.FIELDNAME_ANALYSIS_METHODS, 1);
            if (fIncludeMetadata)
                projection.append(AbstractVariantData.SECTION_ADDITIONAL_INFO, 1);
        }

        for (String spId : sampleIDsNotToExport == null ? sampleIDsToExport : sampleIDsNotToExport)
            projection.append(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + spId, sampleIDsNotToExport == null ? 1 /*include*/ : 0 /*exclude*/);

        if (!projection.isEmpty()) {
            if (sampleIDsNotToExport != null && !fIncludeMetadata)    // exclusive $project (more than a half of the samples are being exported). We only add it if the $project stage is already justified (it's expensive so avoiding it is better when possible)
                projection.append(AbstractVariantData.SECTION_ADDITIONAL_INFO, 0);

            projectStage = new BasicDBObject("$project", projection);
            if (markerCount > 5000 && nTotalNumberOfSamplesInDB > 200 && sampleIDsNotToExport != null && !sampleIDsNotToExport.isEmpty()) {   // we may only attempt evaluating if it's worth removing $project when exported markers are numerous enough, overall sample count is large enough and more than a half of them is involved
                double nTotalChunkCount = Math.ceil(markerCount.intValue() / nQueryChunkSize);
                if (nTotalChunkCount > 30)   // at least 10 chunks would be used for comparison, we only bother doing it if the optimization can be applied to at least 20 others
                    nNumberOfChunksUsedForSpeedEstimation = markerCount == null ? 5 : Math.max(5, (int) nTotalChunkCount / 100 /*1% of the whole stuff*/);
            }
        }
    }
	
	public void setTmpExtractionFolder(String extractionDirToUse) {
		try {
			dataExtractionFolder = new File(extractionDirToUse);
			dataExtractionFolder.mkdirs();
		}
		catch (Exception e) {
			dataExtractionFolder = null;
			LOG.error("Unable to use folder " + extractionDirToUse + " for extaction. Files will be written to the default temp folder.");
		}
	}

    public void readAndWrite(OutputStream os) throws IOException, InterruptedException, ExecutionException {
        if (markerCount == null)
        	throw new IOException("markerCount may not be null");
        
        if (markerCount == 0)
        	return;

        List<String> currentMarkerIDs = new ArrayList<>(nQueryChunkSize);

        VariantRunDataComparator vrdComparator = new VariantRunDataComparator(nAssemblyId);
        
        List<BasicDBObject> pipeline = new ArrayList<>();
        if (variantMatchStage != null)
        	pipeline.add(variantMatchStage);
        pipeline.add(sortStage);
        pipeline.add(new BasicDBObject("$project", new BasicDBObject("_id", 1)));

        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);        
        MongoCollection<Document> collForQueryingIDs = fWorkingOnTempColl ? varColl : mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class));
        MongoCursor<Document> markerCursor = collForQueryingIDs.aggregate(pipeline, Document.class).collation(IExportHandler.collationObj).allowDiskUse(true).batchSize(nQueryChunkSize).iterator();   /*FIXME: didn't find a way to set noCursorTimeOut on aggregation cursors*/
        
        // pipeline reference will be re-used to query VariantRunData
        pipeline = new ArrayList<>();
        pipeline.add(sortStage);
        
        if (markerCount == null)
        	throw new IOException("markerCount may not be null");

        Future<Void>[] chunkExportTasks = new Future[(int) Math.ceil((float) markerCount / nQueryChunkSize)];
        chunkGenotypeFiles = new File[chunkExportTasks.length];
        if (exportWriter.writesVariantFiles())
        	chunkVariantFiles = new File[chunkExportTasks.length];
        chunkWarningFiles = new File[chunkExportTasks.length];
        
        LOG.debug("Exporting from " + varColl.getNamespace().getCollectionName() + " in " + chunkExportTasks.length + " chunks of size " + nQueryChunkSize);
        
        ExecutorService executor = MongoTemplateManager.getExecutor(module);
        String taskGroup = "export_" + System.currentTimeMillis() + "_" + progress.getProcessId();
        
        AtomicInteger nChunkIndex = new AtomicInteger(0), nNextChunkToAppendToMainOS = new AtomicInteger(0), nSpeedEstimationCompletedQueryCount = new AtomicInteger(0);
        AtomicLong timeSpentReadingWithoutProjectStage = new AtomicLong(0), timeSpentReadingWithProjectStage = new AtomicLong(0);
        ConcurrentSkipListSet<Integer> extractedChunks = new ConcurrentSkipListSet<>();
        
        try {
	        MongoCollection<VariantRunData> runColl = mongoTemplate.getDb().withCodecRegistry(ExportManager.pojoCodecRegistry).getCollection(mongoTemplate.getCollectionName(VariantRunData.class), VariantRunData.class);
	        while (markerCursor.hasNext()) {
	            if (progress.isAborted() || progress.getError() != null) {
	        		for (Future<Void> t : chunkExportTasks)
	        			if (t != null)
	        				t.cancel(true);
	                for (File f : chunkWarningFiles)
	                	if (f != null)
	                		f.delete();
	                break;
	            }

	            currentMarkerIDs.add(markerCursor.next().getString("_id"));
	
	            if (currentMarkerIDs.size() >= nQueryChunkSize || !markerCursor.hasNext()) {	// current variant ID list is large enough to start exporting chunk
	            	List<String> chunkMarkerIDs = currentMarkerIDs;
	            	List<BasicDBObject> chunkPipeline = new ArrayList<>(pipeline);
	            	int nFinalChunkIndex = nChunkIndex.incrementAndGet();
	            	Thread chunkExportThread = new Thread() {
	            		public void run() {
	            			if (progress.isAborted() || progress.getError() != null)
	            				return;
	
	            			OutputStream genotypeChunkOS = null, variantChunkOS = null, warningChunkOS = null;
	            			try {
		            			File genotypeChunkFile = File.createTempFile(nFinalChunkIndex + "__genotypes__", "__" + taskGroup, dataExtractionFolder), warningChunkFile = File.createTempFile(nFinalChunkIndex + "__warnings__", "__" + taskGroup, dataExtractionFolder);
		            			chunkGenotypeFiles[nFinalChunkIndex - 1] = genotypeChunkFile;

		            			genotypeChunkOS = new BufferedOutputStream(new FileOutputStream(genotypeChunkFile), 16384);
		            			if (exportWriter.writesVariantFiles()) {
		            				File variantChunkFile = File.createTempFile(nFinalChunkIndex + "__variants__", "__" + taskGroup, dataExtractionFolder);
			            			chunkVariantFiles[nFinalChunkIndex - 1] = variantChunkFile;
			            			variantChunkOS = new BufferedOutputStream(new FileOutputStream(variantChunkFile), 16384);
		            			}
		            			chunkWarningFiles[nFinalChunkIndex - 1] = warningChunkFile;
		            			warningChunkOS = new BufferedOutputStream(new FileOutputStream(warningChunkFile), 16384);
		            			
		                        BasicDBList matchAndList = new BasicDBList();
		                        if (!projectFilterList.isEmpty())
		                            matchAndList.add(projectFilterList.size() == 1 ? projectFilterList.get(0) : new BasicDBObject("$or", projectFilterList));
		                        matchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$in", chunkMarkerIDs)));
		
		                        chunkPipeline.add(0, new BasicDBObject("$match", new BasicDBObject("$and", matchAndList)));
	
	                        	if (projectStage != null && (nNumberOfChunksUsedForSpeedEstimation == null || nFinalChunkIndex <= nNumberOfChunksUsedForSpeedEstimation))
	                                chunkPipeline.add(projectStage);
	
//	                        	if (nFinalChunkIndex == 1)
//	                        		LOG.debug("Export pipeline: " + chunkPipeline);
		
	                			if (progress.isAborted() || progress.getError() != null)
	                				return;

		            			long chunkProcessingStartTime = System.currentTimeMillis();
		                        ArrayList<VariantRunData> runs = runColl.aggregate(chunkPipeline, VariantRunData.class).allowDiskUse(true).into(new ArrayList<>(chunkMarkerIDs.size())); // we don't use collation here because it leads to unexpected behaviour (sometimes fetches some additional variants to those in chunkMarkerIDs) => we'll have to sort each chunk by hand

		                        if (nNumberOfChunksUsedForSpeedEstimation != null) {  // chunkPipeline contains a $project stage that we need to assess: let's compare execution speed with and without it (best option depends on so many things that we can't find it out otherwise)
			                        long chunkProcessingDuration = System.currentTimeMillis() - chunkProcessingStartTime;
	
			                        if (nFinalChunkIndex <= nNumberOfChunksUsedForSpeedEstimation) { // we just tested with $project, let's try without it now
		                                timeSpentReadingWithProjectStage.addAndGet(chunkProcessingDuration);
		                                nSpeedEstimationCompletedQueryCount.incrementAndGet();
		                            }
		                            else if (nFinalChunkIndex <= 2 * nNumberOfChunksUsedForSpeedEstimation) {
		                            	timeSpentReadingWithoutProjectStage.addAndGet(chunkProcessingDuration);
		                                nSpeedEstimationCompletedQueryCount.incrementAndGet();
		                            }
			                        
	                                if (nSpeedEstimationCompletedQueryCount.get() == 2 * nNumberOfChunksUsedForSpeedEstimation) {
	//                            		System.err.println(nFinalChunkIndex + ": timeSpentReadingWithProjectStage: " + timeSpentReadingWithProjectStage.get());
	//	                                System.err.println(nFinalChunkIndex + ": timeSpentReadingWithoutProjectStage: " + timeSpentReadingWithoutProjectStage);
	//	                            	System.err.println(nSpeedEstimationCompletedQueryCount.get() + " / " + nFinalChunkIndex + " -> " + (float) timeSpentReadingWithoutProjectStage.get() / timeSpentReadingWithProjectStage.get());
		                                if ((float) timeSpentReadingWithoutProjectStage.get() / timeSpentReadingWithProjectStage.get() <= .75) {    // removing $project provided more than 25% speed-up : let's do the rest of the export without $project
		                                	projectStage = null;
		                                    LOG.debug("Exporting without $project stage");
		                                }
		                                nNumberOfChunksUsedForSpeedEstimation = null;	// we want to do this only once
	                                }
		                        }
		                        
		                        Collections.sort(runs, vrdComparator);    // make sure variants within this chunk are correctly sorted
		                        
		                        LinkedHashMap<String /*variant ID*/, Collection<VariantRunData>> chunkMarkerRunsToWrite = new LinkedHashMap<>(chunkMarkerIDs.size());
		                        for (String variantId : chunkMarkerIDs)
		                        	chunkMarkerRunsToWrite.put(variantId, null);	// there must be an entry for each exported variant
		                        
		                        String varId = null, previousVarId = null;
		                        List<VariantRunData> currentMarkerRuns = new ArrayList<>(involvedRunCount);
		                        for (VariantRunData vrd : runs) {
		                			if (progress.isAborted() || progress.getError() != null)
		                				return;
	
		                            varId = vrd.getId().getVariantId();
		                            
		                            if (previousVarId != null && !varId.equals(previousVarId)) {
		                                chunkMarkerRunsToWrite.put(previousVarId, currentMarkerRuns);
		                                currentMarkerRuns = new ArrayList<>(involvedRunCount);
		                            }
		                            currentMarkerRuns.add(vrd);
		                            previousVarId = varId;
		                        }
		                        
		                        if (!currentMarkerRuns.isEmpty()) {	// add runs for this chunk's last variant (among those we have data for)
		        	                chunkMarkerRunsToWrite.put(previousVarId, currentMarkerRuns);
		        	                currentMarkerRuns = new ArrayList<>(involvedRunCount);
		                        }

	                            progress.setCurrentStepProgress(extractedChunks.size() * 100l / chunkGenotypeFiles.length);
		                        exportWriter.writeChunkRuns(chunkMarkerRunsToWrite.values(), chunkMarkerIDs, genotypeChunkOS, variantChunkOS, warningChunkOS);
		                        
		        		    	// make sure data is completely written out before the files get read
		        		        genotypeChunkOS.close();
	            				if (variantChunkOS != null)
									variantChunkOS.close();
								warningChunkOS.close();
		                        extractedChunks.add(nFinalChunkIndex - 1);
		                        previousVarId = null;
		                        
		                        // early cleanup to avoid having too many empty files all over the place
		        		        if (exportWriter.writesVariantFiles() && chunkVariantFiles[nFinalChunkIndex - 1] != null && chunkVariantFiles[nFinalChunkIndex - 1].length() == 0) 
		        		        	chunkVariantFiles[nFinalChunkIndex - 1].delete();	// only keep non-empty files
		        		        if (chunkWarningFiles[nFinalChunkIndex - 1] != null && chunkWarningFiles[nFinalChunkIndex - 1].length() == 0) 
		        		        	chunkWarningFiles[nFinalChunkIndex - 1].delete();	// only keep non-empty files

//		        		        synchronized (chunkGenotypeFiles) {
//		                        	while (extractedChunks.contains(nNextChunkToAppendToMainOS.get()) && nNextChunkToAppendToMainOS.get() < chunkGenotypeFiles.length)
//		                        		appendChunkToMainOS(chunkGenotypeFiles[nNextChunkToAppendToMainOS.getAndIncrement()], os);
//		                        }
	
		                        chunkMarkerRunsToWrite = new LinkedHashMap<>(nQueryChunkSize);
	                		}
	                		catch (Exception e) {
	                			if (!(e instanceof MongoInterruptedException && progress.isAborted()) && progress.getError() == null) {
	                				String sError = "Error exporting from " + module;
	                				progress.setError(sError + " - " + e.getMessage());
	                				LOG.error(sError, e);
	                			}
	                			return;
	                		}
	            			finally {
								try {	// these should be already closed if everything went well, so just in case...
									genotypeChunkOS.close();
		            				if (variantChunkOS != null)
										variantChunkOS.close();
									warningChunkOS.close();
								} catch (IOException ignored) {}
	            			}
		            	}
	            	};
	            	
	            	if (nFinalChunkIndex - 1 >= chunkExportTasks.length) {
	            		AtomicInteger nRemaining = new AtomicInteger(0);
	            		markerCursor.forEachRemaining(t -> nRemaining.incrementAndGet());
	            		throw new Exception("All expected (" + chunkExportTasks.length + ") export chunks already processed but markerCursor still had " + nRemaining + " elements! markerCount=" + markerCount + " and " + sampleIDsToExport.size() + " samples");
	            	}

            		chunkExportTasks[nFinalChunkIndex - 1] = (Future<Void>) executor.submit(new TaskWrapper(taskGroup, chunkExportThread));
	                currentMarkerIDs = new ArrayList<>(nQueryChunkSize);
	            }
	        }
	        
	        for (Future<Void> t : chunkExportTasks) // wait for all threads before moving to next phase
	        	if (t != null) {	// we can have null tasks with IGV exports
	        		t.get();

                	while (extractedChunks.contains(nNextChunkToAppendToMainOS.get()) && nNextChunkToAppendToMainOS.get() < chunkGenotypeFiles.length)
                		appendChunkToMainOS(chunkGenotypeFiles[nNextChunkToAppendToMainOS.getAndIncrement()], os);
	        	}

	        progress.insertStep("Merging results");
	        progress.moveToNextStep();
	        int nFirstChunkToAppend = nNextChunkToAppendToMainOS.get();
	        for (int i=nFirstChunkToAppend; i<chunkGenotypeFiles.length; i++) {
		        appendChunkToMainOS(chunkGenotypeFiles[i], os);
	        	progress.setCurrentStepProgress((i - nFirstChunkToAppend) * 100l / (chunkGenotypeFiles.length - nFirstChunkToAppend));
	        }
        }
        catch (Exception e) {
			if (!(e instanceof CancellationException && progress.isAborted()) && progress.getError() == null) {
				String sError = "Error exporting from " + module;
				progress.setError(sError + " - " + e.getMessage());
				LOG.error(sError, e);
			}
    		for (Future<Void> t : chunkExportTasks)
    			if (t != null)
    				t.cancel(true);
    		cleanupOutputFiles();
            return;
        }
        finally {
        	os.flush();
	        markerCursor.close();

	    	// CRITICAL: Always shutdown the group, even if there was an error
	        if (executor instanceof GroupedExecutor)
	        	((GroupedExecutor) executor).shutdown(taskGroup);
	        else
	        	executor.shutdown();

	        for (int i=0; i<chunkGenotypeFiles.length; i++) {
		        if (chunkGenotypeFiles[i] != null && chunkGenotypeFiles[i].exists()) 
		        	chunkGenotypeFiles[i].delete();	// delete all genotype files because if everything went well their contents have been written to the OutputStream
		        if (exportWriter.writesVariantFiles() && chunkVariantFiles[i] != null && chunkVariantFiles[i].length() == 0) 
		        	chunkVariantFiles[i].delete();	// only keep non-empty files
		        if (chunkWarningFiles[i] != null && chunkWarningFiles[i].length() == 0) 
		        	chunkWarningFiles[i].delete();	// only keep non-empty files
	        }
	        
			new Timer().schedule(new TimerTask() {
			    @Override
			    public void run() {
			    	cleanupOutputFiles();
			    }
			}, 1000 * 60 * 2);	// delete remaining files after 2 minutes, in case calling code didn't handle that
        }
    }
    
    private void appendChunkToMainOS(File chunkFile, OutputStream os) throws IOException, InterruptedException {
        if (chunkFile != null) {	// we can have null files with IGV exports
        	try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(chunkFile))) {
    		    byte[] buffer = new byte[8192];
    		    int bytesRead;

    		    while ((bytesRead = bis.read(buffer)) != -1)
    		        os.write(buffer, 0, bytesRead);
    		}
        	chunkFile.delete();
        }
    }
    
    private void cleanupOutputFiles() {
	    for (File f : chunkGenotypeFiles)
        	if (f != null)
        		f.delete();
	    if (chunkVariantFiles != null)
		    for (File f : chunkVariantFiles)
	        	if (f != null)
	        		f.delete();
		for (File f : chunkWarningFiles)
        	if (f != null)
        		f.delete();
    }

	public ExportOutputs getOutputs() {
		return new ExportOutputs(chunkGenotypeFiles, chunkVariantFiles, chunkWarningFiles);
	}
    
    public static class ExportOutputs
    {
        public ExportOutputs(File[] chunkGenotypeFiles, File[] chunkVariantFiles, File[] chunkWarningFiles) {
			super();
			this.chunkGenotypeFiles = chunkGenotypeFiles;
			this.chunkVariantFiles = chunkVariantFiles;
			this.chunkWarningFiles = chunkWarningFiles;
		}

		private File[] chunkGenotypeFiles;
        private File[] chunkVariantFiles;
        private File[] chunkWarningFiles;
        private String metadataFileName;
        private String metadataFileContents;
        boolean workWithSamples = false;

        // may be used to replace variant-oriented files with individual-oriented files
    	public void setGenotypeFiles(File[] chunkGenotypeFiles) {
    		this.chunkGenotypeFiles = chunkGenotypeFiles;
    	}
    	
    	public File[] getGenotypeFiles() {
    		return chunkGenotypeFiles;
    	}
    	
        public File[] getVariantFiles() {
    		return chunkVariantFiles;
    	}

    	public File[] getWarningFiles() {
    		return chunkWarningFiles;
    	}
    	
    	public void setMetadata(String name, String contents) {
    		metadataFileName = name;
    		metadataFileContents = contents;
    	}

		public String getMetadataFileName() {
			return metadataFileName;
		}
		
		public String getMetadataFileContents() {
			return metadataFileContents;
		}

		public void setWorkWithSamples(boolean workWithSamples) {
			this.workWithSamples = workWithSamples;
		}
		
		public boolean isWorkWithSamples() {
			return workWithSamples;
		}
    }
    
    public static abstract class AbstractExportWriter
    {
    	static final Logger LOG = Logger.getLogger(AbstractExportWriter.class);
    	
    	protected boolean fWritesVariantFile = true;

		public AbstractExportWriter() {
		}

		public AbstractExportWriter(boolean fWritesVariantFile) {
			this.fWritesVariantFile = fWritesVariantFile;
		}

    	public boolean writesVariantFiles() {
			return fWritesVariantFile;
		}

		abstract public void writeChunkRuns(Collection<Collection<VariantRunData>> markerRunsToWrite, List<String> orderedMarkerIDs, OutputStream genotypeOS, OutputStream variantOS, OutputStream warningkOS) throws IOException;
    }
}