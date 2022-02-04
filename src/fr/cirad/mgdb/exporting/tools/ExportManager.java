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
import java.util.ArrayDeque;
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
    
    static public final Comparator<VariantRunData> vrdComparator = new Comparator<VariantRunData>() {
        @Override
        public int compare(VariantRunData vrd1, VariantRunData vrd2) {
            if (vrd1.getReferencePosition() == null) {
                if (vrd2.getReferencePosition() == null)
                    return vrd1.getId().getVariantId().compareTo(vrd2.getId().getVariantId());     // none is positioned
                return -1;    // only vrd2 is positioned
            }
            if (vrd2.getReferencePosition() == null)
                return 1;    // only vrd1 is positioned
            
            // both are positioned
            int chrComparison = alphaNumericStringComparator.compare(vrd1.getReferencePosition().getSequence(), vrd2.getReferencePosition().getSequence());
            return chrComparison != 0 ? chrComparison : (int) (vrd1.getReferencePosition().getStartSite() - vrd2.getReferencePosition().getStartSite());
        }
    };

    private ProgressIndicator progress;
    
    private int nQueryChunkSize;
    
    private boolean fWorkingOnTempColl;
    
    private FileWriter warningFileWriter;
    
    private MongoTemplate mongoTemplate;
    
    private AbstractExportWritingThread writingThread;
    
    private Long markerCount;
    
    private Collection<Integer> sampleIDsToExport;
    
    private MongoCollection<Document> varColl;
    
    private Class resultType;
    
    int involvedRunCount;
    
    private BasicDBObject matchStage = null;
    private BasicDBObject sortStage = null;
    private BasicDBObject projectStage = null;
    
    private Integer nNumberOfChunksUsedForSpeedEstimation = null;  // if it remains null then we won't attempt any comparison
    
    private ArrayList<Document> projectFilterList = new ArrayList<>();

    public static final CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().register(new IntKeyMapPropertyCodecProvider()).automatic(true).build()));
    
    public ExportManager(MongoTemplate mongoTemplate, MongoCollection<Document> varColl, Class resultType, Document varQuery, Collection<GenotypingSample> samplesToExport, boolean fIncludeMetadata, int nQueryChunkSize, AbstractExportWritingThread writingThread, Long markerCount, FileWriter warningFileWriter, ProgressIndicator progress) {
        this.progress = progress;
        this.nQueryChunkSize = nQueryChunkSize;
        this.warningFileWriter = warningFileWriter;
        this.mongoTemplate = mongoTemplate;
        this.writingThread = writingThread;
        this.markerCount = markerCount;
        this.varColl = varColl;
        this.resultType = resultType;

        String varCollName = varColl.getNamespace().getCollectionName();
        fWorkingOnTempColl = varCollName.startsWith(MongoTemplateManager.TEMP_COLL_PREFIX);

        String refPosPath = AbstractVariantData.FIELDNAME_REFERENCE_POSITION;
        sortStage = new BasicDBObject("$sort", new Document(refPosPath  + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(refPosPath + "." + ReferencePosition.FIELDNAME_START_SITE, 1));

        // optimization 1: filling in involvedProjectRuns will provide means to apply filtering on project and/or run fields when exporting from temporary collection
        HashMap<Integer, List<String>> involvedProjectRuns = Helper.getRunsByProjectInSampleCollection(samplesToExport);
        involvedRunCount = involvedProjectRuns.values().stream().mapToInt(b -> b.size()).sum();

        int nDbProjectCount = (int) Helper.estimDocCount(mongoTemplate, GenotypingProject.class);
        for (int projId : involvedProjectRuns.keySet()) {
            List<String> projectInvolvedRuns = involvedProjectRuns.get(projId);
            if (projectInvolvedRuns.size() != mongoTemplate.findDistinct(new Query(Criteria.where("_id").is(projId)), GenotypingProject.FIELDNAME_RUNS, GenotypingProject.class, String.class).size()) {
                // not all of this project's runs are involved: only match those (in the case of multi-project and/or multi-run databases, it is worth filtering on these fields to avoid parsing records related to unwanted samples)
                projectFilterList.add(new Document("$and", Arrays.asList(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, projId), new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_RUNNAME, new BasicDBObject("$in", projectInvolvedRuns)))));
            }
            else if (nDbProjectCount != involvedProjectRuns.size())
                projectFilterList.add(new Document("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, projId));    // not all of this DB's projects are involved: only match selected ones
        }

        // optimization 2: build the $project stage by excluding fields when over 50% of overall samples are selected; even remove that stage when exporting all (or almost all) samples (makes it much faster)
        long nTotalNumberOfSamplesInDB = Helper.estimDocCount(mongoTemplate, GenotypingSample.class);
        long percentageOfExportedSamples = nTotalNumberOfSamplesInDB == 0 ? 100 : 100 * samplesToExport.size() / nTotalNumberOfSamplesInDB;
        sampleIDsToExport = samplesToExport == null ? new ArrayList<>() : samplesToExport.stream().map(sp -> sp.getId()).collect(Collectors.toList());
        Collection<Integer> sampleIDsNotToExport = percentageOfExportedSamples >= 98 ? new ArrayList<>() /* if almost all individuals are being exported we directly omit the $project stage */ : (percentageOfExportedSamples > 50 ? mongoTemplate.findDistinct(new Query(Criteria.where("_id").not().in(sampleIDsToExport)), "_id", GenotypingSample.class, Integer.class) : null);

        if (!varQuery.isEmpty()) {
            if (!fWorkingOnTempColl && !projectFilterList.isEmpty()) {
                Entry<String, Object> firstMatchEntry = varQuery.entrySet().iterator().next();
                List<Document> matchAndList = "$and".equals(firstMatchEntry.getKey()) ? (List<Document>) firstMatchEntry.getValue() : Arrays.asList(varQuery);
                matchAndList.addAll(projectFilterList);
            }
            matchStage = new BasicDBObject("$match", varQuery);
        }

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

        for (Integer spId : sampleIDsNotToExport == null ? sampleIDsToExport : sampleIDsNotToExport)
            projection.append(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + spId, sampleIDsNotToExport == null ? 1 /*include*/ : 0 /*exclude*/);

        if (!projection.isEmpty()) {
            if (sampleIDsNotToExport != null && !fIncludeMetadata)    // exclusive $project (more than a half of the samples are being exported). We only add it if the $project stage is already justified (it's expensive so avoiding it is better when possible)
                projection.append(AbstractVariantData.SECTION_ADDITIONAL_INFO, 0);

            projectStage = new BasicDBObject("$project", projection);
            if (markerCount != null && markerCount > 5000 && nTotalNumberOfSamplesInDB > 200 && sampleIDsNotToExport != null && !sampleIDsNotToExport.isEmpty()) {   // we may only attempt evaluating if it's worth removing $project when exported markers are numerous enough, overall sample count is large enough and more than a half of them is involved
                double nTotalChunkCount = Math.ceil(markerCount.intValue() / nQueryChunkSize);
                if (nTotalChunkCount > 30)   // at least 10 chunks would be used for comparison, we only bother doing it if the optimization can be applied to at more than 20 others
                    nNumberOfChunksUsedForSpeedEstimation = markerCount == null ? 5 : Math.max(5, (int) nTotalChunkCount / 100 /*1% of the whole stuff*/);
            }
        }
    }

    public void readAndWrite() throws IOException, InterruptedException, ExecutionException {        
        if (fWorkingOnTempColl)
            exportFromTempColl();
        else
            exportDirectlyFromRuns();
    }
    
    /**
     * Exports by $match-ing successive chunks of variant IDs in VariantRunData. Would have thought using $lookup with a single cursor would be faster, but it's much slower
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void exportFromTempColl() throws IOException, InterruptedException, ExecutionException {
        CompletableFuture<Void> future = null;
        Collection<Collection<VariantRunData>> tempMarkerRunsToWrite = new ArrayList<>(nQueryChunkSize);
        List<VariantRunData> currentMarkerRuns = new ArrayList<>(involvedRunCount);
        List<String> currentMarkerIDs = new ArrayList<>(nQueryChunkSize);
        String varId = null, previousVarId = null;
        int nWrittenmarkerCount = 0;
        
        List<BasicDBObject> pipeline = new ArrayList<>();
        if (matchStage != null)
            pipeline.add(matchStage);   // there can be a $match on temp colls (for example to apply a range when displaying IGV data)
        pipeline.add(sortStage);
        pipeline.add(new BasicDBObject("$project", new BasicDBObject("_id", 1)));

        MongoCursor markerCursor = varColl.aggregate(pipeline, Document.class).collation(IExportHandler.collationObj).allowDiskUse(true).batchSize(nQueryChunkSize).iterator();   /*FIXME: didn't find a way to set noCursorTimeOut on aggregation cursors*/

        // pipeline object will we re-used to query VariantRunData, we won't need the $project stage for that
        pipeline = new ArrayList<>();
        pipeline.add(sortStage);
        
        int nChunkIndex = 0;
        long chunkProcessingStartTime = System.currentTimeMillis(), timeSpentReadingWithoutProjectStage = -1;
        
        MongoCollection<VariantRunData> runColl = mongoTemplate.getDb().withCodecRegistry(ExportManager.pojoCodecRegistry).getCollection(mongoTemplate.getCollectionName(VariantRunData.class), VariantRunData.class);
        while (markerCursor.hasNext()) {
            if (progress.isAborted() || progress.getError() != null ) {
                if (warningFileWriter != null)
                    warningFileWriter.close();
                return;
            }
            
            currentMarkerIDs.add(((Document) markerCursor.next()).getString("_id"));
            
            if (currentMarkerIDs.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
                nChunkIndex++;

                BasicDBList matchAndList = new BasicDBList();
                if (!projectFilterList.isEmpty())
                    matchAndList.add(projectFilterList.size() == 1 ? projectFilterList.get(0) : new BasicDBObject("$or", projectFilterList));
                matchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$in", currentMarkerIDs)));

                BasicDBObject initialMatchStage = new BasicDBObject("$match", new BasicDBObject("$and", matchAndList));
                if (varId == null)
                    pipeline.add(0, initialMatchStage);    // first time we set this stage, it's an insertion
                else
                    pipeline.set(0, initialMatchStage);    // replace existing $match

                if (nChunkIndex == 1) {
                	if (nNumberOfChunksUsedForSpeedEstimation == null && projectStage != null)
                		pipeline.add(projectStage);	// it was decided to use this $project and not to test removing test
                    LOG.debug("Export pipeline: " + pipeline);
                }

                ArrayList<VariantRunData> runs = runColl.aggregate(pipeline, VariantRunData.class).allowDiskUse(true).into(new ArrayList<>(currentMarkerIDs.size())); // we don't use collation here because it leads to unexpected behaviour (sometimes fetches some additional variants to those in currentMarkerIDs) => we'll have to sort each chunk by hand
                Collections.sort(runs, vrdComparator);    // make sure variants within this chunk are correctly sorted
                
                for (VariantRunData vrd : runs) {
                    varId = vrd.getId().getVariantId();
                    
                    if (previousVarId != null && !varId.equals(previousVarId)) {
                        tempMarkerRunsToWrite.add(currentMarkerRuns);
                        currentMarkerRuns = new ArrayList<>(involvedRunCount);
                        nWrittenmarkerCount++;
                    }
                    currentMarkerRuns.add(vrd);
                    previousVarId = varId;
                }

                if (!markerCursor.hasNext()) {    // special case, when the end of the cursor is being reached
                    tempMarkerRunsToWrite.add(currentMarkerRuns);
                    nWrittenmarkerCount++;
                }
                currentMarkerIDs.clear();

                if (nNumberOfChunksUsedForSpeedEstimation != null) {  // pipeline contains a $project stage that we need to assess: let's compare execution speed with and without it (best option depends on so many things that we can't find it out otherwise)
                    if (nChunkIndex == nNumberOfChunksUsedForSpeedEstimation) { // we just tested without $project, let's try with it now
                        timeSpentReadingWithoutProjectStage = System.currentTimeMillis() - chunkProcessingStartTime;
                        pipeline.add(projectStage);
                    }
                    else if (nChunkIndex == 2 * nNumberOfChunksUsedForSpeedEstimation) {
                        long timeSpentReadingWithProjectStage = System.currentTimeMillis() - chunkProcessingStartTime;
                        if (timeSpentReadingWithoutProjectStage < timeSpentReadingWithProjectStage) {    // removing $project provided more some speed-up : let's do the rest of the export without $project
                            pipeline.remove(projectStage);
                            LOG.debug("Exporting without $project stage");
                        }
                    }
                    chunkProcessingStartTime = System.currentTimeMillis();
                }

                if (future != null && !future.isDone()) {
//                    long b4 = System.currentTimeMillis();
                    future.get();
//                    long delay = System.currentTimeMillis() - b4;
//                    if (delay > 100)
//                        LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenmarkerCount/* + ", increasing nQueryChunkSize from " + nQueryChunkSize + " to " + nQueryChunkSize*2*/);
                }
                
                if (markerCount != null)
                    progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
                future = writingThread.writeRuns(tempMarkerRunsToWrite);
                tempMarkerRunsToWrite = new ArrayDeque<>(nQueryChunkSize); 
            }
        }

        if (future != null && !future.isDone())
            future.get();
        markerCursor.close();
        if (markerCount != null)
            progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
    }

    private void exportDirectlyFromRuns() throws IOException, InterruptedException, ExecutionException {
        CompletableFuture<Void> future = null;
        Collection<Collection<VariantRunData>> tempMarkerRunsToWrite = new ArrayDeque<>(nQueryChunkSize);
        List<VariantRunData> currentMarkerRuns = new ArrayList<>();
        String varId = null, previousVarId = null;
        int nWrittenmarkerCount = 0;
        
        List<BasicDBObject> pipeline = new ArrayList<>();
        if (matchStage != null) {
            Document matchContents = (Document) matchStage.get("$match");
            BasicDBList filters = matchContents.containsKey("$and") ? (BasicDBList) matchContents.get("$and") : new BasicDBList() {{ add(new BasicDBObject(matchContents)); }};
            Helper.convertIdFiltersToRunFormat(Arrays.asList(filters));
            pipeline.add(matchStage);
        }
        pipeline.add(sortStage);
        int nPosAfterSortStage = pipeline.size();
        if (projectStage != null)
            pipeline.add(projectStage);
        LOG.debug("Export pipeline: " + pipeline);
        
        final MongoCursor[] markerCursors = new MongoCursor[2]; // First item always exists. Second only exists if we need to compare speed with and without $project stage
        Thread threadCreatingComparisonCursor = null;
        if (nNumberOfChunksUsedForSpeedEstimation != null) {  // pipeline contains a $project stage that we need to assess: let's compare execution speed with and without it (best option depends on so many things that we can't find it out otherwise)
            List<BasicDBObject> pipelineClone = new ArrayList<>(pipeline);
            pipelineClone.add(nPosAfterSortStage, new BasicDBObject("$skip", nQueryChunkSize * nNumberOfChunksUsedForSpeedEstimation));
            threadCreatingComparisonCursor = new Thread() {
                public void run() {                   
                    markerCursors[1] = varColl.aggregate(pipelineClone, resultType).collation(IExportHandler.collationObj).allowDiskUse(true).batchSize(nQueryChunkSize).iterator();   /*FIXME: didn't find a way to set noCursorTimeOut on aggregation cursors*/
                }
            };
            threadCreatingComparisonCursor.start();

            pipeline.remove(pipeline.size() - 1);   // remove $project
        }

        markerCursors[0] = varColl.aggregate(pipeline, resultType).collation(IExportHandler.collationObj).allowDiskUse(true).batchSize(nQueryChunkSize).iterator();   /*FIXME: didn't find a way to set noCursorTimeOut on aggregation cursors*/

        if (threadCreatingComparisonCursor != null)
            threadCreatingComparisonCursor.join();
        
        MongoCursor markerCursor = markerCursors[0];
        int nChunkIndex = 0;
        long chunkProcessingStartTime = System.currentTimeMillis(), timeSpentReadingWithoutProjectStage = -1;
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
                tempMarkerRunsToWrite.add(currentMarkerRuns);    // special case, when the end of the cursor is being reached

            if (tempMarkerRunsToWrite.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
                nChunkIndex++;

                if (markerCursors[1] != null) {
                    if (nChunkIndex == nNumberOfChunksUsedForSpeedEstimation) { // we just tested without $project, let's try with it now
                        timeSpentReadingWithoutProjectStage = System.currentTimeMillis() - chunkProcessingStartTime;
                        markerCursor = markerCursors[1];
                    }
                    else if (nChunkIndex == 2 * nNumberOfChunksUsedForSpeedEstimation) {
                        long timeSpentReadingWithProjectStage = System.currentTimeMillis() - chunkProcessingStartTime;
                        if ((float) timeSpentReadingWithoutProjectStage / timeSpentReadingWithProjectStage <= .75) {    // removing $project provided more than 25% speed-up : let's do the rest of the export without $project
                            markerCursor.close();
                            markerCursor = markerCursors[0];
                            for (int i=0; i<=nQueryChunkSize * nNumberOfChunksUsedForSpeedEstimation; i++)
                                markerCursor.tryNext();
                            LOG.debug("Exporting without $project stage");
                        }
                        else
                            markerCursors[0].close();
                    }
                    chunkProcessingStartTime = System.currentTimeMillis();
                }

                if (future != null && !future.isDone()) {
//                    long b4 = System.currentTimeMillis();
                    future.get();
//                    long delay = System.currentTimeMillis() - b4;
//                    if (delay > 100) {
//                        LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenmarkerCount/* + ", increasing nQueryChunkSize from " + nQueryChunkSize + " to " + nQueryChunkSize*2*/);
                }

                if (markerCount != null && markerCount > 0)
                    progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
                future = writingThread.writeRuns(tempMarkerRunsToWrite);
                tempMarkerRunsToWrite = new ArrayDeque<>(nQueryChunkSize); 
            }
            previousVarId = varId;
        }

        if (future != null && !future.isDone())
            future.get();
        markerCursor.close();
        if (markerCount != null && markerCount > 0)
            progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
    }
}
