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
package fr.cirad.mgdb.model.mongodao;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ejb.ObjectNotFoundException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;

import fr.cirad.mgdb.model.mongo.maintypes.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.brapi.v2.model.VariantSet;
import org.bson.Document;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.ObjectOperators.MergeObjects;
import org.springframework.data.mongodb.core.aggregation.VariableOperators.Let.ExpressionVariable;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata.CustomIndividualMetadataId;
import fr.cirad.mgdb.model.mongo.maintypes.CustomSampleMetadata.CustomSampleMetadataId;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;
import fr.cirad.tools.SessionAttributeAwareThread;
import fr.cirad.tools.mongo.MongoTemplateManager;
import fr.cirad.tools.security.base.AbstractTokenManager;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;

/**
 * The Class MgdbDao.
 */
@Component
public class MgdbDao {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(MgdbDao.class);

    /**
     * The Constant COLLECTION_NAME_TAGGED_VARIANT_IDS.
     */
    static final public String COLLECTION_NAME_TAGGED_VARIANT_IDS = "taggedVariants";
    
    static final public String COLLECTION_NAME_GENE_CACHE = "geneCache";

    /**
     * The Constant FIELD_NAME_CACHED_COUNT_VALUE.
     */
    static final public String FIELD_NAME_CACHED_COUNT_VALUE = "val";
    
    static final public String MESSAGE_TEMP_RECORDS_NOT_FOUND = "Unable to find temporary records: please SEARCH again!";

    @Autowired
    static protected ObjectFactory<HttpSession> httpSessionFactory;

    static protected MgdbDao instance;	// a bit of a hack, allows accessing a singleton to be able to call the non-static loadIndividualsWithAllMetadata

    @Autowired
    private void setMgdbDao(MgdbDao mgdbDao) {
        instance = mgdbDao;
    }

    public static MgdbDao getInstance() {
        return instance;
    }
    
    @Autowired
    private void setHttpSessionFactory(ObjectFactory<HttpSession> hsf) {
        httpSessionFactory = hsf;
    }
    
    public static ArrayList<AbstractVariantData> tagVariants(MongoTemplate mongoTemplate, String sVariantCollName, long totalIndividualCount) {
        long totalVariantCount = mongoTemplate.count(new Query(), sVariantCollName);
        long maxGenotypeCount = totalVariantCount * totalIndividualCount;
        long numberOfTaggedVariants = Math.min(totalVariantCount / 2, maxGenotypeCount > 200000000 ? 500 : (maxGenotypeCount > 100000000 ? 300 : (maxGenotypeCount > 50000000 ? 100 : (maxGenotypeCount > 20000000 ? 50 : (maxGenotypeCount > 5000000 ? 40 : 25)))));
        return tagVariants(mongoTemplate, sVariantCollName, totalVariantCount, numberOfTaggedVariants, null, null, null);
    }

    public static ArrayList<AbstractVariantData> tagVariants(MongoTemplate mongoTemplate, String sVariantCollName, long totalVariantCount, long numberOfTaggedVariants, Criteria crit, Sort sort, Collection<String> projectedFields) {
        /*  This is how it is internally handled when sharding the data:
        var splitKeys = db.runCommand({splitVector: "mgdb_Musa_acuminata_v2_private.variantRunData", keyPattern: {"_id":1}, maxChunkSizeBytes: 40250000}).splitKeys;
        for (var key in splitKeys)
          db.taggedVariants.insert({"_id" : splitKeys[key]["_id"]["vi"]});
         */

        int nChunkSize = (int) Math.max(1, (int) totalVariantCount / Math.max(1, numberOfTaggedVariants - 1));
        LOG.debug("Number of variants between 2 tagged ones: " + nChunkSize);

        AbstractVariantData currentTag = null;
        ArrayList<AbstractVariantData> taggedVariants = new ArrayList<>();
        for (int nChunkNumber = 0; nChunkNumber < (float) totalVariantCount / nChunkSize; nChunkNumber++) {
//            long before = System.currentTimeMillis();
            Query query = crit == null ? new Query() : new Query(crit);
            query.fields().include("_id");
            if (projectedFields != null)
            	for (String field : projectedFields)
            		query.fields().include(field);
            query.limit(nChunkSize);
            query.with(sort != null ? sort : Sort.by(Arrays.asList(new Sort.Order(Sort.Direction.ASC, "_id"))));
            if (currentTag != null)
                query.addCriteria(Criteria.where("_id").gt(currentTag.getVariantId()));
            List<VariantData> chunk = mongoTemplate.find(query, VariantData.class);
            try {
                currentTag = chunk.get(chunk.size() - 1);
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                if (aioobe.getMessage().equals("-1"))
                    LOG.error("Database is mixing String and ObjectID types!");
            }
            taggedVariants.add(currentTag);
//            LOG.debug("Variant " + cursor + " tagged as position " + nChunkNumber + " (" + (System.currentTimeMillis() - before) + "ms)");
        }
        return taggedVariants;
    }

    /**
     * Prepare database for searches.
     *
     * @param sModule the database name
     * @throws Exception
     */
    public static void prepareDatabaseForSearches(String sModule) throws Exception {
    	MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
    	if (!MongoTemplateManager.isModuleAvailableForWriting(sModule)) 
    		throw new Exception("prepareDatabaseForSearches may only be called when database is unlocked");
    	
    	// cleanup unused callsets that possibly got persisted during a failed import
        Collection<Integer> validProjIDs = (Collection<Integer>) mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)).distinct("_id", Integer.class).into(new ArrayList<>());
        DeleteResult dr = mongoTemplate.remove(new Query(Criteria.where(CallSet.FIELDNAME_PROJECT_ID).not().in(validProjIDs)), CallSet.class);
        if (dr.getDeletedCount() > 0)
            LOG.info(dr.getDeletedCount() + " unused samples were removed");
    
        // empty count cache
        mongoTemplate.dropCollection(mongoTemplate.getCollectionName(CachedCount.class));

        MongoCollection<Document> variantColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)), runColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class));

        // make sure positions are indexed with correct collation etc...
        ensurePositionIndexes(mongoTemplate, Arrays.asList(variantColl, runColl), false, false);
        if (!variantColl.find(new BasicDBObject()).projection(new BasicDBObject("_id", 1)).limit(1).cursor().hasNext())
        	throw new NoSuchElementException("No variants found in database!");

    	MongoCollection<Document> taggedVarColl = mongoTemplate.getCollection(COLLECTION_NAME_TAGGED_VARIANT_IDS);
        Thread t = new Thread() {
            public void run() {
                // create indexes
            	ensureVariantDataIndexes(mongoTemplate);

                // tag variant IDs across database
                taggedVarColl.drop();
            	ArrayList<AbstractVariantData> taggedVariants = tagVariants(mongoTemplate, variantColl.getNamespace().getCollectionName(), mongoTemplate.count(new Query(), Individual.class));
                if (!taggedVariants.isEmpty()) {	// otherwise there is apparently no variant in the DB
                	List<Document> docs = taggedVariants.stream().map(var -> new Document("_id", var.getVariantId())).toList();
                	taggedVarColl.insertMany(docs);
                }
            }
        };
        t.start();

        if (mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_EFFECT_ANNOTATIONS + ".0").exists(true)) {{ fields().include("_id"); }}, GenotypingProject.class) == null)
            LOG.debug("Skipping index creation for effect name & gene since database contains no such information");
        else {
            LOG.debug("Creating index on field " + VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE + " of collection " + runColl.getNamespace());
            runColl.createIndex(new BasicDBObject(VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, 1));
            LOG.debug("Creating index on field " + VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME + " of collection " + runColl.getNamespace());
            runColl.createIndex(new BasicDBObject(VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, 1));
        }
        LOG.debug("Creating index on field _id." + VariantRunDataId.FIELDNAME_VARIANT_ID + " of collection " + runColl.getNamespace());
        runColl.createIndex(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, 1));
        LOG.debug("Creating index on field _id." + VariantRunDataId.FIELDNAME_PROJECT_ID + " of collection " + runColl.getNamespace());
        runColl.createIndex(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, 1));
        
        t.join();
        if (taggedVarColl.countDocuments() == 0)
            throw new Exception("An error occured while preparing database for searches, please check server logs");
    }
    
    static public List<String> getVariantTypes(MongoTemplate mongoTemplate, Integer projId) {
        Query q = new Query();
        if (projId != null)
        	q.addCriteria(Criteria.where("_id").is(projId));
        List<String> res = mongoTemplate.findDistinct(q, GenotypingProject.FIELDNAME_VARIANT_TYPES, GenotypingProject.class, String.class);
        return res;
    }
    
    public static int ensureVariantDataIndexes(MongoTemplate mongoTemplate) {
    	return ensureVariantDataIndexes(mongoTemplate, false);	
    }

    /**
     * Ensures VariantData indexes are correct
     *
     * @param mongoTemplate the mongoTemplate
     * @param mongoTemplate if false, skips synonym index creation if none such synonyms found in the first 100000 documents 
     */
    public static int ensureVariantDataIndexes(MongoTemplate mongoTemplate, boolean fEvenIfNoSuchSynonyms) {
    	int nResult = 0;
        MongoCollection<Document> variantColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class));
        
        boolean fFoundTypeIndex = false;
        Collection<String> missingSynonymIndexes = new HashSet<>() {{ 
        	add(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA);
        	add(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL);
        	add(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_NCBI);
        	add(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_AXIOM);
        }}, foundSynonymIndexes = new ArrayList<>();
        
        MongoCursor<Document> indexCursor = variantColl.listIndexes().cursor();
        while (indexCursor.hasNext()) {
            Document doc = (Document) indexCursor.next();
            Document keyDoc = ((Document) doc.get("key"));
            if (keyDoc.keySet().contains(VariantData.FIELDNAME_TYPE)) {
            	fFoundTypeIndex = true;
            	continue;
            }
            for (String indexType : missingSynonymIndexes)
                if (keyDoc.keySet().contains(indexType)) {
                	foundSynonymIndexes.add(indexType);
                	continue;
                }
        }
        missingSynonymIndexes.removeAll(foundSynonymIndexes);

        for (String idx : missingSynonymIndexes)
	        if (fEvenIfNoSuchSynonyms || variantColl.aggregate(Arrays.asList( new BasicDBObject("$limit", 100000), new BasicDBObject("$match", new BasicDBObject(idx + ".0", new BasicDBObject("$exists", true))) )).iterator().hasNext()) {
	            LOG.debug("Creating index on field " + idx + " of collection " + variantColl.getNamespace());
	            variantColl.createIndex(new BasicDBObject(idx, 1));
	            nResult++;
	        }

        if (!fFoundTypeIndex)
	        try {
	            LOG.debug("Creating index on field " + VariantData.FIELDNAME_TYPE + " of collection " + variantColl.getNamespace());
	            variantColl.createIndex(new BasicDBObject(VariantData.FIELDNAME_TYPE, 1));
	            nResult++;
	        } catch (MongoCommandException mce) {
	            if (!mce.getMessage().contains("already exists with a different name")) {
	                throw mce;  // otherwise we have nothing to do because it already exists anyway
	            }
	        }
        return nResult;
    }

    /**
     * Ensures position indexes are correct in passed collections. Supports
     * variants, variantRunData and temporary collections Removes incorrect
     * indexes if necessary
     *
     * @param mongoTemplate the mongoTemplate
     * @param varColls variant collections to ensure indexes on
     * @param fEvenIfCollectionIsEmpty if true, create indexes no matter if collections contains documents
     * @param fWaitForCompletion if true, method does not return before index creation is complete
     * @return the number of indexes that were created
     * @throws InterruptedException 
     */
    public static int ensurePositionIndexes(MongoTemplate mongoTemplate, Collection<MongoCollection<Document>> varColls, boolean fEvenIfCollectionIsEmpty, boolean fWaitForCompletion) throws InterruptedException {
        int nResult = 0;
        
        List<String> variantTypes = getVariantTypes(mongoTemplate, null);
        boolean fOnlySNPsInDB = variantTypes.size() == 1 && Type.SNP.toString().equals(variantTypes.iterator().next());
        	
        List<Assembly> assemblies = mongoTemplate.findAll(Assembly.class);
        List<Integer> asmIDs = !assemblies.isEmpty() ? assemblies.stream().map(asm -> asm.getId()).collect(Collectors.toList()) : new ArrayList() {{ add(null); }};
        for (Integer assemblyId : asmIDs) {
	        String rpPath = Assembly.getVariantRefPosPath(assemblyId) + ".";
	        BasicDBObject startCompoundIndexKeys = new BasicDBObject(rpPath + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(rpPath + ReferencePosition.FIELDNAME_START_SITE, 1), ssIndexKeys = new BasicDBObject(rpPath + ReferencePosition.FIELDNAME_START_SITE, 1);
	        BasicDBObject endCompoundIndexKeys = new BasicDBObject(rpPath + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(rpPath + ReferencePosition.FIELDNAME_END_SITE, 1), esIndexKeys = new BasicDBObject(rpPath + ReferencePosition.FIELDNAME_END_SITE, 1);
	
	        for (MongoCollection<Document> coll : varColls) {
	            boolean fIsTmpColl = coll.getNamespace().getCollectionName().startsWith(MongoTemplateManager.TEMP_COLL_PREFIX);
	            if (!fIsTmpColl && (!fEvenIfCollectionIsEmpty && coll.estimatedDocumentCount() == 0))
	                continue;	// database seems empty: indexes will be created after imports (faster this way) 

	            boolean fFoundStartCompoundIndex = false, fFoundCorrectStartCompoundIndex = false, fFoundStartSiteIndex = false, fFoundEndCompoundIndex = false, fFoundCorrectEndCompoundIndex = false, fFoundEndSiteIndex = false;
	            MongoCursor<Document> indexCursor = coll.listIndexes().cursor();
	            while (indexCursor.hasNext()) {
	                Document doc = (Document) indexCursor.next();
	                Document keyDoc = ((Document) doc.get("key"));
	                Set<String> keyIndex = (Set<String>) keyDoc.keySet();
	                if (keyIndex.size() == 1) {
	                    if ((rpPath + ReferencePosition.FIELDNAME_START_SITE).equals(keyIndex.iterator().next()))
	                        fFoundStartSiteIndex = true;
	                    else if (!fOnlySNPsInDB && (rpPath + ReferencePosition.FIELDNAME_END_SITE).equals(keyIndex.iterator().next()))
	                    	fFoundEndSiteIndex = true;
	                    
//	                    // CLEANUP
//	                    else if (fOnlySNPsInDB && (rpPath + ReferencePosition.FIELDNAME_END_SITE).equals(keyIndex.iterator().next())) {
//	                    	LOG.info("CLEANUP: removing " + keyDoc + " on " + coll.getNamespace());
//	                        coll.dropIndex(keyDoc);
//	                    }
	                } else if (keyIndex.size() == 2) {	// compound index
	                    String[] compoundIndexItems = keyIndex.toArray(new String[2]);
	                    if (compoundIndexItems[0].equals(rpPath + ReferencePosition.FIELDNAME_SEQUENCE) && compoundIndexItems[1].equals(rpPath + ReferencePosition.FIELDNAME_START_SITE)) {
	                        fFoundStartCompoundIndex = true;
	                        Document collation = (Document) doc.get("collation");
	                        fFoundCorrectStartCompoundIndex = collation != null && "en_US".equals(collation.get("locale")) && Boolean.TRUE.equals(collation.get("numericOrdering"));
	                    }
	                    else if (!fOnlySNPsInDB && compoundIndexItems[0].equals(rpPath + ReferencePosition.FIELDNAME_SEQUENCE) && compoundIndexItems[1].equals(rpPath + ReferencePosition.FIELDNAME_END_SITE)) {
	                    	fFoundEndCompoundIndex = true;
	                        Document collation = (Document) doc.get("collation");
	                        fFoundCorrectEndCompoundIndex = collation != null && "en_US".equals(collation.get("locale")) && Boolean.TRUE.equals(collation.get("numericOrdering"));
	                    }
	                    
//	                    // CLEANUP
//	                    else if (fOnlySNPsInDB && compoundIndexItems[0].equals(rpPath + ReferencePosition.FIELDNAME_SEQUENCE) && compoundIndexItems[1].equals(rpPath + ReferencePosition.FIELDNAME_END_SITE)) {
//	                    	LOG.info("CLEANUP: removing " + keyDoc + " on " + coll.getNamespace());
//	                        coll.dropIndex(keyDoc);
//	                    }
//	                    // CLEANUP
//	                    else if ((compoundIndexItems[0].equals(rpPath + ReferencePosition.FIELDNAME_START_SITE) || compoundIndexItems[0].equals(rpPath + ReferencePosition.FIELDNAME_END_SITE)) && compoundIndexItems[1].equals(rpPath + ReferencePosition.FIELDNAME_SEQUENCE)) {
//	                    	LOG.info("CLEANUP: removing " + keyDoc + " on " + coll.getNamespace());
//	                        coll.dropIndex(keyDoc);
//	                    }
	                }
	            }
	            
	            List<Thread> threads = new ArrayList<>();

	            if (!fFoundStartSiteIndex) {
	                Thread ssIndexCreationThread = new Thread() {
	                    public void run() {
	                        LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Creating index " + ssIndexKeys + " on collection " + coll.getNamespace());
	                        coll.createIndex(ssIndexKeys);
	                    }
	                };
	                threads.add(ssIndexCreationThread);
	                nResult++;
	            }
	            
	            if (!fOnlySNPsInDB && !fFoundEndSiteIndex) {
	                Thread esIndexCreationThread = new Thread() {
	                    public void run() {
	                        LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Creating index " + esIndexKeys + " on collection " + coll.getNamespace());
	                        coll.createIndex(esIndexKeys);
	                    }
	                };
	                threads.add(esIndexCreationThread);
	                nResult++;
	            }
	
	            if (!fFoundStartCompoundIndex || (fFoundStartCompoundIndex && !fFoundCorrectStartCompoundIndex)) {
	                final MongoCollection<Document> collToDropCompoundIndexOn = fFoundStartCompoundIndex ? coll : null;
	                Thread ssIndexCreationThread = new Thread() {
	                    public void run() {
	                        if (collToDropCompoundIndexOn != null) {
	                            LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Dropping wrong index " + startCompoundIndexKeys + " on collection " + collToDropCompoundIndexOn.getNamespace());
	                            collToDropCompoundIndexOn.dropIndex(startCompoundIndexKeys);
	                        }
	
	                        LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Creating index " + startCompoundIndexKeys + " on collection " + coll.getNamespace());
	                        coll.createIndex(startCompoundIndexKeys, new IndexOptions().collation(IExportHandler.collationObj));
	                    }
	                };
	                threads.add(ssIndexCreationThread);
	
	                nResult++;
	            }

	            if (!fOnlySNPsInDB && (!fFoundEndCompoundIndex || (fFoundEndCompoundIndex && !fFoundCorrectEndCompoundIndex))) {
	                final MongoCollection<Document> collToDropCompoundIndexOn = fFoundEndCompoundIndex ? coll : null;
	                Thread esIndexCreationThread = new Thread() {
	                    public void run() {
	                        if (collToDropCompoundIndexOn != null) {
	                            LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Dropping wrong index " + endCompoundIndexKeys + " on collection " + collToDropCompoundIndexOn.getNamespace());
	                            collToDropCompoundIndexOn.dropIndex(endCompoundIndexKeys);
	                        }
	
	                        LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Creating index " + endCompoundIndexKeys + " on collection " + coll.getNamespace());
	                        coll.createIndex(endCompoundIndexKeys, new IndexOptions().collation(IExportHandler.collationObj));
	                    }
	                };
	                threads.add(esIndexCreationThread);
	
	                nResult++;
	            }

		        for (Thread t : threads)
		        	t.start();

		        if (fWaitForCompletion)
		        	for (Thread t : threads)
			        	t.join();
	        }
        }
        return nResult;
    }

    public static boolean idLooksGenerated(String id) {
        return id.length() == 20 && id.matches("^[0-9a-f]+$");
    }

    /**
     * Estimate number of variants to query at once.
     *
     * @param totalNumberOfMarkersToQuery the total number of markers to query
     * @param nNumberOfWantedGenotypes the n number of wanted genotypes
     * @return the int
     * @throws Exception the exception
     */
    public static int estimateNumberOfVariantsToQueryAtOnce(int totalNumberOfMarkersToQuery, int nNumberOfWantedGenotypes) throws Exception {
        if (totalNumberOfMarkersToQuery <= 0) {
            throw new Exception("totalNumberOfMarkersToQuery must be >0");
        }

        int nSampleCount = Math.max(1 /*in case someone would pass 0 or less*/, nNumberOfWantedGenotypes);
        int nResult = Math.max(1, 200000 / nSampleCount);

        return Math.min(nResult, totalNumberOfMarkersToQuery);
    }

    /**
     * Gets the sample genotypes.
     *
     * @param mongoTemplate the mongo template
     * @param variantFieldsToReturn the variant fields to return
     * @param projectIdToReturnedRunFieldListMap the project id to returned run field list map
     * @param variantIdListToRestrictTo the variant id list to restrict to
     * @param sort the sort
     * @return the sample genotypes
     * @throws Exception the exception
     */
    private static LinkedHashMap<VariantData, Collection<VariantRunData>> getSampleGenotypes(MongoTemplate mongoTemplate, ArrayList<String> variantFieldsToReturn, HashMap<Integer, ArrayList<String>> projectIdToReturnedRunFieldListMap, List<String> variantIdListToRestrictTo, Sort sort) throws Exception {
        Query variantQuery = new Query();
        if (sort != null) {
            variantQuery.with(sort);
        }

        Criteria runQueryVariantCriteria = null;

        if (variantIdListToRestrictTo != null && variantIdListToRestrictTo.size() > 0) {
            variantQuery.addCriteria(new Criteria().where("_id").in(variantIdListToRestrictTo));
            runQueryVariantCriteria = new Criteria().where("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID).in(variantIdListToRestrictTo);
        }
        variantQuery.fields().include("_id");
        for (String returnedField : variantFieldsToReturn) {
            variantQuery.fields().include(returnedField);
        }

        HashMap<String, VariantData> variantIdToVariantMap = new HashMap<>();
        List<VariantData> variants = mongoTemplate.find(variantQuery, VariantData.class);
        for (VariantData vd : variants) {
            variantIdToVariantMap.put(vd.getId(), vd);
        }

        // next block may be removed at some point (only some consistency checking)
        if (variantIdListToRestrictTo != null && variantIdListToRestrictTo.size() != variants.size()) {
            mainLoop:
            for (Object vi : variantIdListToRestrictTo) {
                for (VariantData vd : variants) {
                    if (!variantIdToVariantMap.containsKey(vd.getId())) {
                        variantIdToVariantMap.put(vd.getId(), vd);
                    }

                    if (vd.getId().equals(vi)) {
                        continue mainLoop;
                    }
                }
                LOG.error(vi + " requested but not returned");
            }
            throw new Exception("Found " + variants.size() + " variants where " + variantIdListToRestrictTo.size() + " were expected");
        }

        LinkedHashMap<VariantData, Collection<VariantRunData>> result = new LinkedHashMap<VariantData, Collection<VariantRunData>>();
        for (Object variantId : variantIdListToRestrictTo) {
            result.put(variantIdToVariantMap.get(variantId.toString()), new ArrayDeque<VariantRunData>());
        }

        for (int projectId : projectIdToReturnedRunFieldListMap.keySet()) {
            Query runQuery = new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(projectId));
            if (runQueryVariantCriteria != null) {
                runQuery.addCriteria(runQueryVariantCriteria);
            }

            runQuery.fields().include("_id");
            for (String returnedField : projectIdToReturnedRunFieldListMap.get(projectId)) {
                runQuery.fields().include(returnedField);
            }

            List<VariantRunData> runs = mongoTemplate.find(runQuery, VariantRunData.class);
            for (VariantRunData run : runs) {
                result.get(variantIdToVariantMap.get(run.getId().getVariantId())).add(run);
            }
        }

        if (result.size() != variantIdListToRestrictTo.size()) {
            throw new Exception("Bug: we should be returning " + variantIdListToRestrictTo.size() + " results but we only have " + result.size());
        }

        return result;
    }

    /**
     * Gets the sample genotypes.
     *
     * @param mongoTemplate the mongo template
     * @param samples the samples
     * @param variantIdListToRestrictTo the variant id list to restrict to
     * @param fReturnVariantTypes whether or not to return variant types
     * @param sort the sort
     * @return the sample genotypes
     * @throws Exception the exception
     */
    public static LinkedHashMap<VariantData, Collection<VariantRunData>> getSampleGenotypes(MongoTemplate mongoTemplate, Collection<CallSet> samples, List<String> variantIdListToRestrictTo, boolean fReturnVariantTypes, Sort sort) throws Exception {
        ArrayList<String> variantFieldsToReturn = new ArrayList<String>();
        variantFieldsToReturn.add(VariantData.FIELDNAME_KNOWN_ALLELES);
        variantFieldsToReturn.add(VariantData.FIELDNAME_REFERENCE_POSITION);
        if (fReturnVariantTypes) {
            variantFieldsToReturn.add(VariantData.FIELDNAME_TYPE);
        }

        HashMap<Integer /*project id*/, ArrayList<String>> projectIdToReturnedRunFieldListMap = new HashMap<Integer, ArrayList<String>>();
        for (CallSet sample : samples) {
            ArrayList<String> returnedFields = projectIdToReturnedRunFieldListMap.get(sample.getProjectId());
            if (returnedFields == null) {
                returnedFields = new ArrayList<String>();
                returnedFields.add("_class");
                returnedFields.add(VariantRunData.SECTION_ADDITIONAL_INFO);
                projectIdToReturnedRunFieldListMap.put(sample.getProjectId(), returnedFields);
            }
            returnedFields.add(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + sample.getId());
        }

        LinkedHashMap<VariantData, Collection<VariantRunData>> result = getSampleGenotypes(mongoTemplate, variantFieldsToReturn, projectIdToReturnedRunFieldListMap, variantIdListToRestrictTo, sort);

        return result;
    }

    public static Set<String> getProjectIndividuals(String sModule, int projId) throws ObjectNotFoundException {
        return getSamplesByIndividualForProject(sModule, projId, null).keySet();
    }

    public static Set<String> getProjectSamples(String sModule, int projId) throws ObjectNotFoundException {
        return getCallsetsBySampleForProject(sModule, projId, null).keySet();
    }

    /**
     * Gets the individuals from samples.
     *
     * @param sModule the module
     * @param samples the sample ids
     * @return the individuals from samples
     */
    public static List<Individual> getIndividualsFromSamples(final String sModule, final Collection<GenotypingSample> samples) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        ArrayList<Individual> result = new ArrayList<Individual>();
        for (GenotypingSample sp : samples) {
            result.add(mongoTemplate.findById(sp.getIndividual(), Individual.class));
        }
        return result;
    }

    public static TreeMap<String /*individual*/, ArrayList<GenotypingSample>> getSamplesByIndividualForProject(final String sModule, final int projId, final Collection<String> individuals) throws ObjectNotFoundException {
        TreeMap<String /*individual*/, ArrayList<GenotypingSample>> result = new TreeMap<>();
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate == null)
            throw new ObjectNotFoundException("Database " + sModule + " does not exist");

        List<String> samples = mongoTemplate.findDistinct(
                new Query().addCriteria(Criteria.where(CallSet.FIELDNAME_PROJECT_ID).is(projId)),
                CallSet.FIELDNAME_SAMPLE,
                CallSet.class,
                String.class
        );

        Criteria crit = Criteria.where("_id").in(samples);
        if (individuals != null)
            crit.andOperator(Criteria.where(GenotypingSample.FIELDNAME_INDIVIDUAL).in(individuals));
        Query q = new Query(crit);

        for (GenotypingSample sample : mongoTemplate.find(q, GenotypingSample.class)) {
            ArrayList<GenotypingSample> individualSamples = result.get(sample.getIndividual());
            if (individualSamples == null) {
                individualSamples = new ArrayList<>();
                result.put(sample.getIndividual(), individualSamples);
            }
            individualSamples.add(sample);
        }
        return result;
    }


    public static ArrayList<GenotypingSample> getSamplesForProject(final String sModule, final int projId, final Collection<String> individuals) throws ObjectNotFoundException {
        ArrayList<GenotypingSample> result = new ArrayList<>();
        for (ArrayList<GenotypingSample> sampleList : getSamplesByIndividualForProject(sModule, projId, individuals).values()) {
            result.addAll(sampleList);
        }
        return result;
    }

    public static TreeMap<String /*individual*/, ArrayList<CallSet>> getCallsetsByIndividualForProject(final String sModule, final int projId, final Collection<String> individuals) throws ObjectNotFoundException {
        TreeMap<String /*individual*/, ArrayList<CallSet>> result = new TreeMap<>();
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate == null)
            throw new ObjectNotFoundException("Database " + sModule + " does not exist");

        Criteria crit = Criteria.where(CallSet.FIELDNAME_PROJECT_ID).is(projId);
        if (individuals != null)
            crit.andOperator(Criteria.where(CallSet.FIELDNAME_INDIVIDUAL).in(individuals));
        Query q = new Query(crit);
//		q.with(new Sort(Sort.Direction.ASC, GenotypingSample.SampleId.FIELDNAME_INDIVIDUAL));
        for (CallSet cs : mongoTemplate.find(q, CallSet.class)) {
            ArrayList<CallSet> individualCallsets = result.get(cs.getIndividual());
            if (individualCallsets == null) {
                individualCallsets = new ArrayList<>();
                result.put(cs.getIndividual(), individualCallsets);
            }
            individualCallsets.add(cs);
        }
        return result;
    }

    public static TreeMap<String /*sample*/, ArrayList<CallSet>> getCallsetsBySampleForProject(final String sModule, final int projId, final Collection<String> samples) throws ObjectNotFoundException {
        TreeMap<String /*sample*/, ArrayList<CallSet>> result = new TreeMap<>();
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate == null)
            throw new ObjectNotFoundException("Database " + sModule + " does not exist");

        Criteria crit = Criteria.where(CallSet.FIELDNAME_PROJECT_ID).is(projId);
        if (samples != null)
            crit.andOperator(Criteria.where(CallSet.FIELDNAME_SAMPLE).in(samples));
        Query q = new Query(crit);
//		q.with(new Sort(Sort.Direction.ASC, GenotypingSample.SampleId.FIELDNAME_INDIVIDUAL));
        for (CallSet cs : mongoTemplate.find(q, CallSet.class)) {
            ArrayList<CallSet> individualCallsets = result.get(cs.getIndividual());
            if (individualCallsets == null) {
                individualCallsets = new ArrayList<>();
                result.put(cs.getIndividual(), individualCallsets);
            }
            individualCallsets.add(cs);
        }
        return result;
    }

    public static ArrayList<CallSet> getCallsetsForProjectAndSamples(final String sModule, final int projId, final Collection<String> samples) throws ObjectNotFoundException {
        ArrayList<CallSet> result = new ArrayList<>();
        for (ArrayList<CallSet> sampleList : getCallsetsBySampleForProject(sModule, projId, samples).values()) {
            result.addAll(sampleList);
        }
        return result;
    }

    public static ArrayList<CallSet> getCallsetsForProjectAndIndividuals(final String sModule, final int projId, final Collection<String> individuals) throws ObjectNotFoundException {
        ArrayList<CallSet> result = new ArrayList<>();
        for (ArrayList<CallSet> sampleList : getCallsetsByIndividualForProject(sModule, projId, individuals).values()) {
            result.addAll(sampleList);
        }
        return result;
    }

    public static List<String> getProjectRunsFromSamples(final String sModule, final int projId, final Collection<String> samples) throws ObjectNotFoundException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);

        Criteria crit = Criteria.where(CallSet.FIELDNAME_PROJECT_ID).is(projId);
        if (samples != null)
            crit.andOperator(Criteria.where(CallSet.FIELDNAME_SAMPLE).in(samples));
        return mongoTemplate.findDistinct(
                new Query(crit),
                CallSet.FIELDNAME_RUN,
                CallSet.class,
                String.class
        );
    }

    public static List<CallSet> getCallSetsFromSamples(final String sModule, final Collection<String> samples) throws ObjectNotFoundException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        List<CallSet> result = mongoTemplate.find(
                    new Query(Criteria.where(CallSet.FIELDNAME_SAMPLE).in(samples)),
                    CallSet.class
                );
        return result;
    }

    
    public static Map<String /*sample name*/, GenotypingSample> getSamplesByIDs(final String sModule, final Collection<Integer> sampleIDs, boolean detachSamplesFromIndividuals) {
        Map<String, GenotypingSample> map = new HashMap<>();
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (sampleIDs != null && sampleIDs.size() > 0) {
            Criteria crit = Criteria.where("_id").in(sampleIDs);
            List<GenotypingSample> samples = mongoTemplate.find(new Query(crit), GenotypingSample.class);
            if (detachSamplesFromIndividuals)
            	for (GenotypingSample sp : samples)	// hack them so each sample is considered separately
            		sp.setDetached(true);  
            map = samples.stream().collect(Collectors.toMap(GenotypingSample::getId, sample -> sample));
        }
        return map;
    }

    /**
     * Gets individuals' populations.
     *
     * @param sModule the module
     * @param individuals the individual IDs
     * @return the individual ID to population map
     */
    public static Map<String, String> getIndividualPopulations(final String sModule, final Collection<String> individuals) {
        return getIndividualPopulations(sModule, individuals, null);
    }

    public static Map<String, String> getIndividualPopulations(final String sModule, final Collection<String> individuals, final String metadataFielToUseAsPop) {
    	String targetedField = metadataFielToUseAsPop == null ? Individual.FIELDNAME_POPULATION : (Individual.SECTION_ADDITIONAL_INFO + "." + metadataFielToUseAsPop);
        Query query = new Query(new Criteria().andOperator(Criteria.where("_id").in(individuals), Criteria.where(targetedField).ne(null)));
        query.fields().include(targetedField);
        return MongoTemplateManager.get(sModule).find(query, Individual.class).stream().collect(Collectors.toMap(ind -> ind.getId(), ind -> metadataFielToUseAsPop == null ? ind.getPopulation() : ind.getAdditionalInfo().values().toArray()[0].toString()));            
    }

    public static TreeSet<String> getAnnotationFields(MongoTemplate mongoTemplate, int projId, boolean fOnlySearchableFields) {
        TreeSet<String> result = new TreeSet<>();

        // we can't use Spring queries here (leads to "Failed to instantiate htsjdk.variant.vcf.VCFInfoHeaderLine using constructor NO_CONSTRUCTOR with arguments")
        MongoCollection<org.bson.Document> vcfHeaderColl = mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(DBVCFHeader.class));
        Document vcfHeaderQuery = new Document("_id." + VcfHeaderId.FIELDNAME_PROJECT, projId);
        MongoCursor<Document> headerCursor = vcfHeaderColl.find(vcfHeaderQuery).iterator();

        while (headerCursor.hasNext()) {
            DBVCFHeader vcfHeader = DBVCFHeader.fromDocument(headerCursor.next());
            for (String key : vcfHeader.getmFormatMetaData().keySet()) {
                VCFFormatHeaderLine vcfFormatHeaderLine = vcfHeader.getmFormatMetaData().get(key);
                if (!fOnlySearchableFields || (!key.equals(VCFConstants.GENOTYPE_KEY) && vcfFormatHeaderLine.getType().equals(VCFHeaderLineType.Integer) && vcfFormatHeaderLine.getCountType() == VCFHeaderLineCount.INTEGER && vcfFormatHeaderLine.getCount() == 1)) {
                    result.add(key);
                }
            }
        }
        headerCursor.close();
        return result;
    }
    
    /**
     * @param module the database name (mandatory)
     * @param sCurrentUser username for whom to get custom metadata (optional)
     * @param projIDs a list of project IDs (optional)
     * @param indIDs a list of individual IDs (optional), has priority over projIDs
     * @param filters the filters to apply (optional)
     * @return Individual IDs mapped to Individual objects with static metada +
     * custom metadata (if available). If indIDs is specified the list is
     * restricted by it, otherwise if projIDs is specified the list is
     * restricted by it, otherwise all database Individuals are returned
     */
    public LinkedHashMap<String, Individual> loadIndividualsWithAllMetadata(String module, String sCurrentUser, Collection<Integer> projIDs, Collection<String> indIDs, LinkedHashMap<String, Set<String>> filters) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
        List<Criteria> crits = new ArrayList<>();
        
        if (indIDs == null && Helper.estimDocCount(mongoTemplate, CallSet.class) != 1) {// if no list of individuals is provided we may select them by project
            List<String> sampleIds = mongoTemplate.findDistinct(projIDs == null || projIDs.isEmpty() ? new Query() : new Query(Criteria.where(CallSet.FIELDNAME_PROJECT_ID).in(projIDs)), CallSet.FIELDNAME_SAMPLE, CallSet.class, String.class);
            indIDs = mongoTemplate.findDistinct(projIDs == null || projIDs.isEmpty() ? new Query() : new Query(Criteria.where("_id").in(sampleIds)), GenotypingSample.FIELDNAME_INDIVIDUAL, GenotypingSample.class, String.class);
        }
        if (indIDs != null)
        	crits.add(Criteria.where("_id").in(indIDs));
 
        if (sCurrentUser != null && !"anonymousUser".equals(sCurrentUser)) {	// merge with custom metadata if available
        	LinkedHashMap<String, Individual> result = new LinkedHashMap<>();
            List<AggregationOperation> pipeline = new ArrayList<>();
        	
        	if (!crits.isEmpty()) 
        		pipeline.add(Aggregation.match(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()]))));
 
        	pipeline.add(LookupOperation.newLookup()
                    .from(MongoTemplateManager.getMongoCollectionName(CustomIndividualMetadata.class))
                    .let(ExpressionVariable.newVariable("id").forField("$_id"))
                    .pipeline(context -> {
                        return new Document("$match",
                                new Document("$expr",
                                        new Document("$and", Arrays.asList(
                                                new Document("$eq", Arrays.asList("$$id", "$_id." + CustomIndividualMetadataId.FIELDNAME_INDIVIDUAL_ID)),
                                                new Document("$eq", Arrays.asList("$_id." + CustomIndividualMetadataId.FIELDNAME_USER, sCurrentUser))
                                        ))
                                ));
                    })
        	.as("cimd"));            	
        	
        	pipeline.add(Aggregation.unwind("$cimd", true));
        	pipeline.add(Aggregation.project().and(MergeObjects.mergeValuesOf("$" + Individual.SECTION_ADDITIONAL_INFO, "$cimd." + Individual.SECTION_ADDITIONAL_INFO)).as(Individual.SECTION_ADDITIONAL_INFO));

            if (filters != null)
    	        for (Entry<String, Set<String>> filterEntry : filters.entrySet()) {
    	            Set<String> filterValues = filterEntry.getValue();
    	            if (!filterValues.isEmpty())
    	            	pipeline.add(Aggregation.match(Criteria.where(Individual.SECTION_ADDITIONAL_INFO + "." + filterEntry.getKey()).in(filterValues)));
    	        }

            AggregationResults<Individual> aggregationResults = mongoTemplate.aggregate(Aggregation.newAggregation(pipeline), MongoTemplateManager.getMongoCollectionName(Individual.class), Individual.class);
            for (Individual ind : aggregationResults.getMappedResults())
            	result.put(ind.getId(), ind);
            return result;
        }
        else {
            boolean fGrabSessionAttributesFromThread = SessionAttributeAwareThread.class.isAssignableFrom(Thread.currentThread().getClass());
            LinkedHashMap<String, LinkedHashMap<String, Object>> sessionMetaData = (LinkedHashMap<String, LinkedHashMap<String, Object>>) (fGrabSessionAttributesFromThread ? ((SessionAttributeAwareThread) Thread.currentThread()).getSessionAttributes().get("individuals_metadata_" + module) : httpSessionFactory.getObject().getAttribute("individuals_metadata_" + module));
            boolean fMergeSessionData = sCurrentUser != null && sessionMetaData != null;

            if (filters != null && !fMergeSessionData)
    	        for (Entry<String, Set<String>> filterEntry : filters.entrySet()) {
    	            Set<String> filterValues = filterEntry.getValue();
    	            if (!filterValues.isEmpty())
    	            	crits.add(Criteria.where(Individual.SECTION_ADDITIONAL_INFO + "." + filterEntry.getKey()).in(filterValues));
    	        }
            
        	// load "official" metadata (that is, those attached to Individual objects)
            Query q = crits.size() == 0 ? new Query() : new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()])));
            q.with(Sort.by(Sort.Direction.ASC, "_id"));
            LinkedHashMap<String, Individual> result = mongoTemplate.find(q, Individual.class).stream().collect(Collectors.toMap(Individual::getId, Function.identity(), (u, v) -> u, LinkedHashMap::new));
            
            if (fMergeSessionData) {	// filters will be applied "by hand" after merging
            	sessionMetaData.entrySet().stream().forEach(indEntry -> {
                    LinkedHashMap<String, Object> indSessionMetadata = sessionMetaData.get(indEntry.getKey());
                    Individual ind = result.computeIfAbsent(indEntry.getKey(), k -> new Individual(indEntry.getKey()));
                    ind.getAdditionalInfo().putAll(indSessionMetadata);
	
	                if (filters != null)
                		for (Entry<String, Set<String>> filterEntry : filters.entrySet()) {
    	            		if (!filterEntry.getValue().isEmpty() && !filterEntry.getValue().contains(ind.getAdditionalInfo().get(filterEntry.getKey()))) {
    	            			result.remove(indEntry.getKey());
    	            			return;
    	            		}
	                }
            	});
            }
            return result;
        }
    }

    /**
     * @param module the database name (mandatory)
     * @param sCurrentUser username for whom to get custom metadata (optional)
     * @param projIDs a list of project IDs (optional)
     * @param spIDs a list of sample IDs (optional)
     * @param filters the filters to apply (optional)
     * @return sample IDs mapped to sample objects with static metada +
     * custom metadata (if available). If spIDs is specified the list is
     * restricted by it, otherwise if projIDs is specified the list is
     * restricted by it, otherwise all database samples are returned
     */
    public LinkedHashMap<String, GenotypingSample> loadSamplesWithAllMetadata(String module, String sCurrentUser, Collection<Integer> projIDs, Collection<String> spIDs, LinkedHashMap<String, Set<String>> filters) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);

        // build the initial list of Sample objects
        if (spIDs == null)
            spIDs = mongoTemplate.findDistinct(projIDs == null || projIDs.isEmpty() ? new Query() : new Query(Criteria.where(CallSet.FIELDNAME_PROJECT_ID).in(projIDs)), CallSet.FIELDNAME_SAMPLE, CallSet.class, String.class);

        List<Criteria> crits = new ArrayList<>();
       	crits.add(Criteria.where("_id").in(spIDs));
       	
        if (filters != null)
	        for (Entry<String, Set<String>> filterEntry : filters.entrySet()) {
	            Set<String> filterValues = filterEntry.getValue();
	            if (!filterValues.isEmpty())
	            	crits.add(Criteria.where(Individual.SECTION_ADDITIONAL_INFO + "." + filterEntry.getKey()).in(filterValues));
	        }
        
        LinkedHashMap<String, GenotypingSample> result = new LinkedHashMap<>();	// this one will be sorted according to the provided list

        Query q = new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()])));
        q.with(Sort.by(Sort.Direction.ASC, "_id"));
        Map<String, GenotypingSample> spMap = mongoTemplate.find(q, GenotypingSample.class).stream().collect(Collectors.toMap(GenotypingSample::getId, sp -> sp));
        for (String spId : spIDs)
            result.put(spId, spMap.get(spId));

        boolean fGrabSessionAttributesFromThread = SessionAttributeAwareThread.class.isAssignableFrom(Thread.currentThread().getClass());
        LinkedHashMap<String, LinkedHashMap<String, Object>> sessionMetaData = (LinkedHashMap<String, LinkedHashMap<String, Object>>) (fGrabSessionAttributesFromThread ? ((SessionAttributeAwareThread) Thread.currentThread()).getSessionAttributes().get("samples_metadata_" + module) : httpSessionFactory.getObject().getAttribute("samples_metadata_" + module));
        if (sCurrentUser != null) {	// merge with custom metadata if available
            if ("anonymousUser".equals(sCurrentUser)) {
            	if (sessionMetaData != null)
	                for (String spID : spIDs) {
	                    LinkedHashMap<String, Object> spSessionMetadata = sessionMetaData.get(spID);
	                    if (spSessionMetadata != null && !spSessionMetadata.isEmpty())
	                        result.get(spID).getAdditionalInfo().putAll(spSessionMetadata);
	                }
            } else
                for (CustomSampleMetadata cimd : mongoTemplate.find(new Query(new Criteria().andOperator(Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_USER).is(sCurrentUser), Criteria.where("_id." + CustomSampleMetadataId.FIELDNAME_SAMPLE_ID).in(spIDs))), CustomSampleMetadata.class))
                    if (cimd.getAdditionalInfo() != null && !cimd.getAdditionalInfo().isEmpty())
                        result.get(cimd.getId().getSampleId()).getAdditionalInfo().putAll(cimd.getAdditionalInfo());
        }
        return result;
    }
//    
//    public static List<Integer> getUserReadableProjectsIds(AbstractTokenManager tokenManager, String token, String sModule, boolean getReadable) throws ObjectNotFoundException {
//        boolean fGotDBRights = tokenManager.canUserReadDB(token, sModule);
//        if (fGotDBRights) {
//            Query q = new Query();
//            q.fields().include(GenotypingProject.FIELDNAME_NAME);
//            List<GenotypingProject> listProj = MongoTemplateManager.get(sModule).find(q, GenotypingProject.class);
//            List<Integer> projIds = listProj.stream().map(p -> p.getId()).collect(Collectors.toList());
//            List<Integer> readableProjIds = new ArrayList<>();
//            for (Integer id : projIds) {
//                if (tokenManager.canUserReadProject(token, sModule, id)) {
//                    readableProjIds.add(id);
//                }
//            }
//            if (getReadable) {
//                return readableProjIds;
//            } else {
//                projIds.removeAll(readableProjIds);
//                return projIds;
//            }
//           
//        } else {
//            throw new ObjectNotFoundException(sModule);
//        }
//    }
    
    public static List<Integer> getUserReadableProjectsIds(AbstractTokenManager tokenManager, Collection<? extends GrantedAuthority> authorities, String sModule, boolean getReadable) throws ObjectNotFoundException {
        boolean fGotDBRights = tokenManager.canUserReadDB(authorities, sModule);
        if (fGotDBRights) {
            Query q = new Query();
            q.fields().include(GenotypingProject.FIELDNAME_NAME);
            List<GenotypingProject> listProj = MongoTemplateManager.get(sModule).find(q, GenotypingProject.class);
            List<Integer> projIds = listProj.stream().map(p -> p.getId()).collect(Collectors.toList());
            List<Integer> readableProjIds = new ArrayList<>();
            for (Integer id : projIds) {
                if (tokenManager.canUserReadProject(authorities, sModule, id)) {
                    readableProjIds.add(id);
                }
            }
            if (getReadable) {
                return readableProjIds;
            } else {
                projIds.removeAll(readableProjIds);
                return projIds;
            }
           
        } else {
            throw new ObjectNotFoundException(sModule);
        }
    }
    
    public static List<GenotypingSample> getSamplesFromIndividualIds(String sModule, List<String> indIDs) {
        Query q = new Query(Criteria.where(GenotypingSample.FIELDNAME_INDIVIDUAL).in(indIDs));
        List<GenotypingSample> samples = MongoTemplateManager.get(sModule).find(q, GenotypingSample.class);
        return samples;
    }

    public static boolean removeProjectAndRelatedRecords(String sModule, int nProjectId) throws Exception {
    	AtomicBoolean fAnythingRemoved = new AtomicBoolean(false);
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        Query query = new Query();
        query.fields().include("_id");
        Collection<String> individualsInThisProject = null, individualsInOtherProjects = new ArrayList<>();
        Collection<String> samplesInThisProject = null, samplesInOtherProjects = new ArrayList<>();
        int nProjCount = 0;
        for (GenotypingProject proj : mongoTemplate.find(query, GenotypingProject.class)) {
            nProjCount++;
            if (proj.getId() == nProjectId) {
                individualsInThisProject = MgdbDao.getProjectIndividuals(sModule, proj.getId());
                samplesInThisProject = MgdbDao.getProjectSamples(sModule, proj.getId());
            } else {
                individualsInOtherProjects.addAll(MgdbDao.getProjectIndividuals(sModule, proj.getId()));
                individualsInOtherProjects.addAll(MgdbDao.getProjectSamples(sModule, proj.getId()));
            }
        }
        if (nProjCount == 1 && !individualsInThisProject.isEmpty()) {
            mongoTemplate.getDb().drop();
            LOG.info("Dropped database for module " + sModule + " instead of removing its only project");
            return true;
        }

        long nRemovedCallsetsCount = mongoTemplate.remove(new Query(Criteria.where(CallSet.FIELDNAME_PROJECT_ID).is(nProjectId)), CallSet.class).getDeletedCount();
        if (nRemovedCallsetsCount > 0) {
	        LOG.info("Removed " + nRemovedCallsetsCount + " callsets for project " + nProjectId);
	        fAnythingRemoved.set(true);
        }

        Collection<String> samplesToRemove = CollectionUtils.disjunction(samplesInThisProject, CollectionUtils.intersection(samplesInThisProject, samplesInOtherProjects));
        long nRemovedSampleCount = mongoTemplate.remove(new Query(Criteria.where("_id").is(samplesToRemove)), GenotypingSample.class).getDeletedCount();
        if (nRemovedSampleCount > 0) {
            LOG.info("Removed " + nRemovedSampleCount + " samples for project " + nProjectId);
            fAnythingRemoved.set(true);
        }

        Collection<String> individualsToRemove = CollectionUtils.disjunction(individualsInThisProject, CollectionUtils.intersection(individualsInThisProject, individualsInOtherProjects));
        long nRemovedIndCount = mongoTemplate.remove(new Query(Criteria.where("_id").in(individualsToRemove)), Individual.class).getDeletedCount();
        if (nRemovedIndCount > 0) {
        	LOG.info("Removed " + nRemovedIndCount + " individuals out of " + individualsInThisProject.size());
	        fAnythingRemoved.set(true);
        }

        if (mongoTemplate.remove(new Query(Criteria.where("_id").is(nProjectId)), GenotypingProject.class).getDeletedCount() > 0) {
            LOG.info("Removed project " + nProjectId + " from module " + sModule);
	        fAnythingRemoved.set(true);
        }

        long nDeletedVcfHeaders = mongoTemplate.remove(new Query(Criteria.where("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_PROJECT).is(nProjectId)), DBVCFHeader.class).getDeletedCount();
        if (nDeletedVcfHeaders > 0) {
            LOG.info("Removed " + nDeletedVcfHeaders + " vcf header(s) for project" + nProjectId + " from module " + sModule);
	        fAnythingRemoved.set(true);
        }

        long nDeletedVariantSetCacheItems = mongoTemplate.remove(new Query(Criteria.where("_id").regex("^" + Helper.createId(sModule, nProjectId, "") + "\\.*")), VariantSet.BRAPI_CACHE_COLL_VARIANTSET).getDeletedCount();
		if (nDeletedVariantSetCacheItems > 0) {
			LOG.debug("Removed " + nDeletedVariantSetCacheItems + " previously existing entries in " + VariantSet.BRAPI_CACHE_COLL_VARIANTSET + " in project " + nProjectId + " of module " + sModule);
			fAnythingRemoved.set(true);
        }
		
		if (mongoTemplate.count(new Query(), MgdbDao.COLLECTION_NAME_GENE_CACHE) > 0) {
	        Update update = new Update();
	        update.pull(Run.FIELDNAME_PROJECT_ID, nProjectId);
	        UpdateResult projRefRemovalResult = mongoTemplate.updateMulti(new Query(), update, MgdbDao.COLLECTION_NAME_GENE_CACHE);
	        if (projRefRemovalResult.getModifiedCount() > 0)
	        	LOG.debug("Removed " + projRefRemovalResult.getModifiedCount() + " project references in gene cache of module " + sModule);
	        DeleteResult geneCacheRemovalResult = mongoTemplate.remove(new Query(Criteria.where(Run.FIELDNAME_PROJECT_ID).size(0)), MgdbDao.COLLECTION_NAME_GENE_CACHE);
	        if (geneCacheRemovalResult.getDeletedCount() > 0)
	        	LOG.debug("Removed " + geneCacheRemovalResult.getDeletedCount() + " gene cache entries from module " + sModule);
		}

        new Thread() {
            public void run() {
                long nRemovedVrdCount = mongoTemplate.remove(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(nProjectId)), VariantRunData.class).getDeletedCount();
                if (nRemovedVrdCount > 0) {
	                LOG.info("Removed " + nRemovedVrdCount + " VRD records for project " + nProjectId + " of module " + sModule);
	    	        fAnythingRemoved.set(true);
                }
            }
        }.start();
        LOG.info("Launched async VRD cleanup for project " + nProjectId + " of module " + sModule);
        
        Update update = new Update();
        update.pull(VariantData.FIELDNAME_RUNS, new Query(Criteria.where(Run.FIELDNAME_PROJECT_ID).is(nProjectId)));
        UpdateResult projRefRemovalResult = mongoTemplate.updateMulti(new Query(Criteria.where(VariantData.FIELDNAME_RUNS + "." + Run.FIELDNAME_PROJECT_ID).is(nProjectId)), update, VariantData.class);
        if (projRefRemovalResult.getModifiedCount() > 0)
        	LOG.info("Removed " + projRefRemovalResult.getModifiedCount() + " project references in variants collection of module " + sModule);


        if (httpSessionFactory == null)
        	LOG.info("Skipped removal of BrAPI v2 VariantSet export files (apparently invoked from command line)");
        else {
	        ServletContext sc = SessionAttributeAwareThread.class.isAssignableFrom(Thread.currentThread().getClass()) ? ((SessionAttributeAwareThread) Thread.currentThread()).getServletContext() : httpSessionFactory.getObject().getServletContext();
	        File brapiV2ExportFolder = new File(sc.getRealPath(File.separator + VariantSet.TMP_OUTPUT_FOLDER));
	        if (brapiV2ExportFolder.exists() && brapiV2ExportFolder.isDirectory())
	        	for (File exportFile : brapiV2ExportFolder.listFiles(f -> f.getName().startsWith(VariantSet.brapiV2ExportFilePrefix + sModule + Helper.ID_SEPARATOR + nProjectId + Helper.ID_SEPARATOR)))
	        		if (exportFile.delete()) {
	        			LOG.info("Deleted BrAPI v2 VariantSet export file: " + exportFile);
				        fAnythingRemoved.set(true);
				    }
        }

        if (!fAnythingRemoved.get())
        	return false;

        mongoTemplate.getCollection(mongoTemplate.getCollectionName(CachedCount.class)).drop();
        MongoTemplateManager.updateDatabaseLastModification(sModule);
        return true;
    }

    public static boolean removeRunAndRelatedRecords(String sModule, int nProjectId, String sRun) throws Exception {
    	AtomicBoolean fAnythingRemoved = new AtomicBoolean(false);
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        long nRemovedCallsetCount = mongoTemplate.remove(new Query(new Criteria().andOperator(Criteria.where(CallSet.FIELDNAME_PROJECT_ID).is(nProjectId), Criteria.where(CallSet.FIELDNAME_RUN).is(sRun))), CallSet.class).getDeletedCount();
        if (nRemovedCallsetCount > 0) {
            LOG.info("Removed " + nRemovedCallsetCount + " callsets for project " + nProjectId + " of module " + sModule);
            fAnythingRemoved.set(true);
        }

        Collection<String> samplesWithCallsets = mongoTemplate.findDistinct(new Query(), CallSet.FIELDNAME_SAMPLE,  CallSet.class, String.class);
        long nRemovedSampleCount = mongoTemplate.remove(new Query(Criteria.where("_id").not().in(samplesWithCallsets)), GenotypingSample.class).getDeletedCount();
        if (nRemovedSampleCount > 0) {
            LOG.info("Removed " + nRemovedSampleCount + " samples from project " + nProjectId + " of module " + sModule);
            fAnythingRemoved.set(true);
        }

        Collection<String> individualsWithSamples = mongoTemplate.findDistinct(new Query(), GenotypingSample.FIELDNAME_INDIVIDUAL,  GenotypingSample.class, String.class);
        long nRemovedIndCount = mongoTemplate.remove(new Query(Criteria.where("_id").not().in(individualsWithSamples)), Individual.class).getDeletedCount();
        if (nRemovedIndCount > 0) {
        	LOG.info("Removed " + nRemovedIndCount + " individuals from project " + nProjectId + " of module " + sModule);
        	fAnythingRemoved.set(true);
        }

        if (mongoTemplate.updateFirst(new Query(Criteria.where("_id").is(nProjectId)), new Update().pull(GenotypingProject.FIELDNAME_RUNS, sRun), GenotypingProject.class).getModifiedCount() > 0) {
            LOG.info("Removed run " + sRun + " from project " + nProjectId + " of module " + sModule);
            fAnythingRemoved.set(true);
            
    		// we have no other option than re-creating the entire gene-cache because we don't keep track of which project run(s) refer to the genes 
    		if (Helper.estimDocCount(mongoTemplate, MgdbDao.COLLECTION_NAME_GENE_CACHE) > 0)
    			new Thread() {
    				public void run() {
                    	MongoNamespace activeNameSpace = mongoTemplate.getCollection(MgdbDao.COLLECTION_NAME_GENE_CACHE).getNamespace(), backupNameSpace = new MongoNamespace(activeNameSpace.getDatabaseName(), activeNameSpace.getCollectionName() + "_old");
                    	mongoTemplate.getCollection(MgdbDao.COLLECTION_NAME_GENE_CACHE).renameCollection(backupNameSpace);
    	                try {
    	            		MgdbDao.createGeneCacheIfNecessary(sModule, MgdbDao.COLLECTION_NAME_GENE_CACHE);
    	            		mongoTemplate.getCollection(backupNameSpace.getCollectionName()).drop();
    	                } catch (Exception e) {
    	                	mongoTemplate.getCollection(MgdbDao.COLLECTION_NAME_GENE_CACHE).drop();
    	                	mongoTemplate.getCollection(backupNameSpace.getCollectionName()).renameCollection(activeNameSpace);
    						LOG.error("Error while re-creating gene-cache collection for db " + sModule, e);
    					}
    	    		}
    	    	}.start();
        }

        if (mongoTemplate.remove(new Query(new Criteria().andOperator(Criteria.where("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_PROJECT).is(nProjectId), Criteria.where("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_RUN).is(sRun))), DBVCFHeader.class).getDeletedCount() > 0) {
            LOG.info("Removed vcf header for run " + sRun + " in project " + nProjectId + " of module " + sModule);
            fAnythingRemoved.set(true);
        }

		if (mongoTemplate.remove(new Query(Criteria.where("_id").is(Helper.createId(sModule, nProjectId, sRun))), VariantSet.BRAPI_CACHE_COLL_VARIANTSET).getDeletedCount() > 0) {
			LOG.debug("Removed previously existing entry in " + VariantSet.BRAPI_CACHE_COLL_VARIANTSET + " for run " + sRun + " in project " + nProjectId + " of module " + sModule);
			fAnythingRemoved.set(true);
        }
		
        new Thread() {
            public void run() {
                long nRemovedVrdCount = mongoTemplate.remove(new Query(new Criteria().andOperator(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(nProjectId), Criteria.where("_id." + VariantRunDataId.FIELDNAME_RUNNAME).is(sRun))), VariantRunData.class).getDeletedCount();
                if (nRemovedVrdCount > 0) {
	                LOG.info("Removed " + nRemovedVrdCount + " VRD records for project " + nProjectId + " of module " + sModule);
	                fAnythingRemoved.set(true);
	            }
            }
        }.start();
        LOG.info("Launched async VRD cleanup for run " + sRun + " in project " + nProjectId + " of module " + sModule);

        if (httpSessionFactory == null)
        	LOG.info("Skipped removal of BrAPI v2 VariantSet export file (apparently invoked from command line)");
        else {
	        ServletContext sc = SessionAttributeAwareThread.class.isAssignableFrom(Thread.currentThread().getClass()) ? ((SessionAttributeAwareThread) Thread.currentThread()).getServletContext() : httpSessionFactory.getObject().getServletContext();
	        File brapiV2ExportFolder = new File(sc.getRealPath(File.separator + VariantSet.TMP_OUTPUT_FOLDER));
	        if (brapiV2ExportFolder.exists() && brapiV2ExportFolder.isDirectory())
	        	for (File exportFile : brapiV2ExportFolder.listFiles(f -> f.getName().startsWith(VariantSet.brapiV2ExportFilePrefix + sModule + Helper.ID_SEPARATOR + nProjectId + Helper.ID_SEPARATOR)))
	        		if (exportFile.delete()) {
	        			LOG.info("Deleted BrAPI v2 VariantSet export file: " + exportFile);
	        			fAnythingRemoved.set(true);
	        		}
        }

        if (!fAnythingRemoved.get())
        	return false;

        mongoTemplate.getCollection(mongoTemplate.getCollectionName(CachedCount.class)).drop();
        MongoTemplateManager.updateDatabaseLastModification(sModule);
        return true;
    }

    /* WARNING; this method works, but if you invoke it, be sure it no import process is running, because it may delete records that are being imported */
    public long removeOrphanVariantRunDataRecords(String sModule) throws Exception {
    	MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
    	if (!MongoTemplateManager.isModuleAvailableForWriting(sModule)) 
    		throw new Exception("removeOrphanVariantRunDataRecords may only be called when database is unlocked");
    	
    	MongoTemplateManager.lockModuleForWriting(sModule);

    	ArrayList<Criteria> norList = new ArrayList<>();
    	for (GenotypingProject project : mongoTemplate.findAll(GenotypingProject.class))
    		norList.add(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()).andOperator(Criteria.where("_id." + GenotypingProject.FIELDNAME_RUNS).in(project.getRuns())));
    	
    	long n = mongoTemplate.remove(new Query(new Criteria().norOperator(norList)), VariantRunData.class).getDeletedCount();;
    	MongoTemplateManager.unlockModuleForWriting(sModule);
    	return n;
    }

    /**
     * @param module the database name (mandatory)
     * @param sCurrentUser username for whom to get custom metadata (optional)
     * @param projID a project ID (optional)
     * @param indIDs a list of individual IDs (optional)
     * @return LinkedHashMap which contains all different metadata of the project
     */
    public LinkedHashMap<String, Set<String>> distinctIndividualMetadata(String module, String sCurrentUser, Integer projID, Collection<String> indIDs) {
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>();	// this one will be sorted according to the provided list
        HashSet<String> wrongFormatFields = new HashSet<>();
        MgdbDao.getInstance().loadIndividualsWithAllMetadata(module, sCurrentUser, Arrays.asList(projID), indIDs, null).values().stream().filter(ind -> ind.getAdditionalInfo() != null).map(ind -> {
        	for (String fieldName : ind.getAdditionalInfo().keySet()) {
        		Object val = ind.getAdditionalInfo().get(fieldName);
        		if (val instanceof String)
        			result.computeIfAbsent(fieldName, k -> new HashSet<>()).add((String) val);
        		else if (wrongFormatFields.add(fieldName))
        			LOG.info("Database " + module + ": Individual metadata type is not String for field " + fieldName);
        	}
			return null;
        }).count();

        return result;
    }

    /**
     * @param module the database name (mandatory)
     * @param sCurrentUser username for whom to get custom metadata (optional)
     * @param projID a project ID (optional)
     * @param spIDs a list of sample IDs (optional)
     * @return LinkedHashMap which contains all different metadata of the project
     */
	public LinkedHashMap<String, Set<String>> distinctSampleMetadata(String module, String sCurrentUser, Integer projID, Collection<String> spIDs) {
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>();	// this one will be sorted according to the provided list
        HashSet<String> wrongFormatFields = new HashSet<>();
        MgdbDao.getInstance().loadSamplesWithAllMetadata(module, sCurrentUser, Arrays.asList(projID), spIDs, null).values().stream().filter(ind -> ind.getAdditionalInfo() != null).map(sp -> {
        	for (String fieldName : sp.getAdditionalInfo().keySet()) {
        		Object val = sp.getAdditionalInfo().get(fieldName);
        		if (val instanceof String)
        			result.computeIfAbsent(fieldName, k -> new HashSet<>()).add((String) val);
        		else if (wrongFormatFields.add(fieldName))
        			LOG.info("Database " + module + ": Sample metadata type is not String for field " + fieldName);
        	}
			return null;
        }).count();

        return result;
	}

	
    public static void addRunsToVariantCollectionIfNecessary(MongoTemplate mongoTemplate) throws Exception {
        String variantDataCollName = mongoTemplate.getCollectionName(VariantData.class), copyCollectionName = variantDataCollName + "_copy";
        String info = "Ensuring presence of run info in variants collection for db " + mongoTemplate.getDb().getName();
        
        List<Document> runsInVariantColl = mongoTemplate.findDistinct(new Query(), VariantData.FIELDNAME_RUNS, VariantData.class, Document.class);
        AggregationResults<Document> runsInDB = mongoTemplate.aggregate(Aggregation.newAggregation(Aggregation.unwind("$" + GenotypingProject.FIELDNAME_RUNS), Aggregation.group(Fields.fields().and(VariantRunDataId.FIELDNAME_PROJECT_ID, "$_id").and(GenotypingProject.FIELDNAME_RUNS, "$" + GenotypingProject.FIELDNAME_RUNS))).withOptions(AggregationOptions.builder().allowDiskUse(true).build()),  GenotypingProject.class, Document.class);
        if (runsInVariantColl.size() >= runsInDB.getMappedResults().size()) {
	        LOG.debug(info + ": variants collection already up to date"); // no need to migrate
	        return;
        }
        
        long before = System.currentTimeMillis();
        
        mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).aggregate(Arrays.asList(new BasicDBObject("$out", copyCollectionName))).allowDiskUse(true).toCollection();
        LOG.info(info + " - Backed-up variant collection: " + Helper.estimDocCount(mongoTemplate, copyCollectionName) + " documents copied into " + copyCollectionName + " collection");

	    ExecutorService executor = Executors.newFixedThreadPool(5);
        MongoNamespace activeNameSpace = mongoTemplate.getCollection(variantDataCollName).getNamespace(), backupNameSpace = new MongoNamespace(activeNameSpace.getDatabaseName(), activeNameSpace.getCollectionName() + "_without_runs");
        final List<Map> taggedVariantList = mongoTemplate.findAll(Map.class, MgdbDao.COLLECTION_NAME_TAGGED_VARIANT_IDS);
        for (int i=0; i<=taggedVariantList.size(); i++) {
        	final int finalIndex = i;
        	executor.submit(new Thread() {
    			public void run() {
    	        	BasicDBObject chunkMatchFilters = new BasicDBObject();
    		        String leftBound = null, rightBound = null;
    		        if (finalIndex > 0) {
    		            leftBound = (String) taggedVariantList.get(finalIndex - 1).get("_id");
    		            chunkMatchFilters.append("$gt", leftBound);
    		        }
    		        
    		        if (finalIndex < taggedVariantList.size()) {
    		            rightBound = (String) taggedVariantList.get(finalIndex).get("_id");
    		            chunkMatchFilters.append("$lte", rightBound);
    		        }
    		        List<BasicDBObject> pipeline = Arrays.asList(
    		        		new BasicDBObject("$match", new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, chunkMatchFilters)),
    	        			new BasicDBObject("$group", new BasicDBObject("_id", "$_id." + VariantRunDataId.FIELDNAME_VARIANT_ID).append(VariantData.FIELDNAME_RUNS, new BasicDBObject("$addToSet", new BasicDBObject(VariantRunDataId.FIELDNAME_PROJECT_ID, "$_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).append(VariantRunDataId.FIELDNAME_RUNNAME, "$_id." + VariantRunDataId.FIELDNAME_RUNNAME)))),
    	        			new BasicDBObject("$merge", new BasicDBObject("into", copyCollectionName).append("whenMatched", "merge").append("whenNotMatched", "discard"))
    	        		);

    		        mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class)).aggregate(pipeline).allowDiskUse(true).toCollection();
    		        if (finalIndex > 0 && finalIndex % 50 == 0)
    		        	LOG.debug(info + " - Adding run info to variants collection..." + (finalIndex * 100 / taggedVariantList.size()) + "%");
    	        }
        	});
        }
        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
        LOG.info(info + " - VariantData documents updated with run info extracted from variantRunData. Update done in " + (System.currentTimeMillis() - before) / 1000 + "s");
        
        try {
        	mongoTemplate.getCollection(variantDataCollName).renameCollection(backupNameSpace);
        }
        catch (MongoCommandException mce) {
        	if (mce.getMessage().contains("NamespaceExists")) {
        		mongoTemplate.getCollection(backupNameSpace.getCollectionName()).drop();
        		mongoTemplate.getCollection(variantDataCollName).renameCollection(backupNameSpace);
        	}
        	else {
        		LOG.error("Error while renaming collections at end of addRunsToVariantCollectionIfNecessary execution", mce);
        		return;
        	}
        }

        mongoTemplate.getCollection(copyCollectionName).renameCollection(activeNameSpace);
		ensureVariantDataIndexes(mongoTemplate);

        MongoTemplateManager.updateDatabaseLastModification(mongoTemplate);
    }
    
    public static void createGeneCacheIfNecessary(String sModule, String sOutputCollectionName) throws Exception {
    	MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate.count(new Query(), sOutputCollectionName) == 0 && mongoTemplate.exists(new Query(Criteria.where(GenotypingProject.FIELDNAME_EFFECT_ANNOTATIONS).ne(new ArrayList<>())), "projects")) {
        	LOG.info("Creating gene cache for db " + mongoTemplate.getDb().getName());
        	long before = System.currentTimeMillis();
        	String geneFieldPath = VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE;
            Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.unwind(geneFieldPath),
                Aggregation.group(geneFieldPath).addToSet("_id." + Run.FIELDNAME_PROJECT_ID).as(Run.FIELDNAME_PROJECT_ID),
                Aggregation.out(sOutputCollectionName)
            );
            mongoTemplate.aggregate(aggregation, mongoTemplate.getCollectionName(VariantRunData.class), Object.class);
            MongoTemplateManager.updateDatabaseLastModification(sModule);
            LOG.info("Creation of gene cache for db " + mongoTemplate.getDb().getName() + " took " + (System.currentTimeMillis() - before)/1000 + "s");
        }
    }

	public static void ensureCustomMetadataIndexes(MongoTemplate mongoTemplate) {
		MongoCollection<Document> coll = mongoTemplate.getCollection(mongoTemplate.getCollectionName(CustomIndividualMetadata.class));
		if (coll.estimatedDocumentCount() == 0)
			return;	// no such data

		String individualIdField = "_id." + CustomIndividualMetadataId.FIELDNAME_INDIVIDUAL_ID;
        MongoCursor<Document> indexCursor = coll.listIndexes().cursor();
        while (indexCursor.hasNext()) {
            Document doc = (Document) indexCursor.next();
            Document keyDoc = ((Document) doc.get("key"));
            Set<String> keyIndex = (Set<String>) keyDoc.keySet();
            if (keyIndex.size() == 1 && individualIdField.equals(keyIndex.iterator().next()))
            	return;	// index already exists
        }
        LOG.debug("Creating index on field " + individualIdField + " of collection " + coll.getNamespace());
        coll.createIndex(new BasicDBObject(individualIdField, 1));
	}
}