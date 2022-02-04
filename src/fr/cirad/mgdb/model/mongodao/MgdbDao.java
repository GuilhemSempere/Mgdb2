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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.exporting.IExportHandler.SessionAttributeAwareExportThread;
import fr.cirad.mgdb.model.mongo.maintypes.CachedCount;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata.CustomIndividualMetadataId;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.mongo.MongoTemplateManager;
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

    /**
     * The Constant FIELD_NAME_CACHED_COUNT_VALUE.
     */
    static final public String FIELD_NAME_CACHED_COUNT_VALUE = "val";

    @Autowired
    protected ObjectFactory<HttpSession> httpSessionFactory;

    static protected MgdbDao instance;	// a bit of a hack, allows accessing a singleton to be able to call the non-static loadIndividualsWithAllMetadata

    @Autowired
    private void setMgdbDao(MgdbDao mgdbDao) {
        instance = mgdbDao;
    }

    public static MgdbDao getInstance() {
        return instance;
    }

    /**
     * Prepare database for searches.
     *
     * @param mongoTemplate the mongo template
     * @return the list
     * @throws Exception
     */
    public static List<String> prepareDatabaseForSearches(MongoTemplate mongoTemplate) throws Exception {
        // cleanup unused sample that eventually got persisted during a failed import
        Collection<Integer> validProjIDs = (Collection<Integer>) mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)).distinct("_id", Integer.class).into(new ArrayList<>());
        DeleteResult dr = mongoTemplate.remove(new Query(Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).not().in(validProjIDs)), GenotypingSample.class);
        if (dr.getDeletedCount() > 0) {
            LOG.info(dr.getDeletedCount() + " unused samples were removed");
        }

        // empty count cache
        mongoTemplate.dropCollection(mongoTemplate.getCollectionName(CachedCount.class));

        MongoCollection<Document> runColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class));

        // make sure positions are indexed with correct collation etc...
        ensurePositionIndexes(mongoTemplate, Arrays.asList(mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)), mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class))));
        
        List<String> result = new ArrayList<>();
        Thread t = new Thread() {
            public void run() {
                // create indexes
                MongoCollection<Document> variantColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class));
                LOG.debug("Creating index on field " + VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA + " of collection " + variantColl.getNamespace());
                variantColl.createIndex(new BasicDBObject(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA, 1));
                LOG.debug("Creating index on field " + VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL + " of collection " + variantColl.getNamespace());
                variantColl.createIndex(new BasicDBObject(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA, 1));
                LOG.debug("Creating index on field " + VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_NCBI + " of collection " + variantColl.getNamespace());
                variantColl.createIndex(new BasicDBObject(VariantData.FIELDNAME_SYNONYMS + "." + VariantData.FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA, 1));
                LOG.debug("Creating index on field " + VariantData.FIELDNAME_TYPE + " of collection " + variantColl.getNamespace());
                try {
                    variantColl.createIndex(new BasicDBObject(VariantData.FIELDNAME_TYPE, 1));
                } catch (MongoCommandException mce) {
                    if (!mce.getMessage().contains("already exists with a different name")) {
                        throw mce;  // otherwise we have nothing to do because it already exists anyway
                    }
                }

                // tag variant IDs across database
                mongoTemplate.dropCollection(COLLECTION_NAME_TAGGED_VARIANT_IDS);
                long totalVariantCount = mongoTemplate.count(new Query(), VariantData.class);
                long totalIndividualCount = mongoTemplate.count(new Query(), Individual.class);
                long maxGenotypeCount = totalVariantCount * totalIndividualCount;
                long numberOfTaggedVariants = Math.min(totalVariantCount / 2, maxGenotypeCount > 200000000 ? 500 : (maxGenotypeCount > 100000000 ? 300 : (maxGenotypeCount > 50000000 ? 100 : (maxGenotypeCount > 20000000 ? 50 : (maxGenotypeCount > 5000000 ? 40 : 25)))));
                int nChunkSize = (int) Math.max(1, (int) totalVariantCount / Math.max(1, numberOfTaggedVariants - 1));
                LOG.debug("Number of variants between 2 tagged ones: " + nChunkSize);

                MongoCollection<Document> collection = mongoTemplate.getCollection(MgdbDao.COLLECTION_NAME_TAGGED_VARIANT_IDS);
                collection.drop();
                String cursor = null;
                ArrayList<Document> taggedVariants = new ArrayList<>();
                for (int nChunkNumber = 0; nChunkNumber < (float) totalVariantCount / nChunkSize; nChunkNumber++) {
                    long before = System.currentTimeMillis();
                    Query q = new Query();
                    q.fields().include("_id");
                    q.limit(nChunkSize);
                    q.with(Sort.by(Arrays.asList(new Sort.Order(Sort.Direction.ASC, "_id"))));
                    if (cursor != null) {
                        q.addCriteria(Criteria.where("_id").gt(cursor));
                    }
                    List<VariantData> chunk = mongoTemplate.find(q, VariantData.class);
                    try {
                        cursor = chunk.get(chunk.size() - 1).getId();
                    } catch (ArrayIndexOutOfBoundsException aioobe) {
                        if (aioobe.getMessage().equals("-1")) {
                            LOG.error("Database is mixing String and ObjectID types!");
                            result.clear();
                        }
                    }
                    taggedVariants.add(new Document("_id", cursor));
                    result.add(cursor.toString());
                    LOG.debug("Variant " + cursor + " tagged as position " + nChunkNumber + " (" + (System.currentTimeMillis() - before) + "ms)");
                }
                collection.insertMany(taggedVariants);
            }
            
            /*  This is how it is internally handled when sharding the data:
            var splitKeys = db.runCommand({splitVector: "mgdb_Musa_acuminata_v2_private.variantRunData", keyPattern: {"_id":1}, maxChunkSizeBytes: 40250000}).splitKeys;
            for (var key in splitKeys)
              db.taggedVariants.insert({"_id" : splitKeys[key]["_id"]["vi"]});
             */
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
//		LOG.debug("Creating index on fields _id." + VariantRunDataId.FIELDNAME_VARIANT_ID + ", _id." + VariantRunDataId.FIELDNAME_PROJECT_ID + " of collection " + runColl.getName());
//		BasicDBObject runCollIndexKeys = new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, 1);
//		runCollIndexKeys.put("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, 1);
//		runColl.createIndex(runCollIndexKeys);
        
        t.join();
        if (result.isEmpty())
            throw new Exception("An error occured while preparing database for searches, please check server logs");
        return result;
    }

    /**
     * Ensures position indexes are correct in passed collections. Supports
     * variants, variantRunData and temporary collections Removes incorrect
     * indexes if necessary
     *
     * @param mongoTemplate the mongoTemplate
     * @param varColls variant collections to ensure indexes on
     * @return the number of indexes that were created
     */
    public static int ensurePositionIndexes(MongoTemplate mongoTemplate, Collection<MongoCollection<Document>> varColls) {
        int nResult = 0;
        String rpPath = VariantData.FIELDNAME_REFERENCE_POSITION + ".";
        BasicDBObject coumpoundIndexKeys = new BasicDBObject(rpPath + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(rpPath + ReferencePosition.FIELDNAME_START_SITE, 1), ssIndexKeys = new BasicDBObject(rpPath + ReferencePosition.FIELDNAME_START_SITE, 1);

        for (MongoCollection<Document> coll : varColls) {
            boolean fIsTmpColl = coll.getNamespace().getCollectionName().startsWith(MongoTemplateManager.TEMP_COLL_PREFIX);
            if (!fIsTmpColl && coll.estimatedDocumentCount() == 0) {
                continue;	// database seems empty: indexes will be created after imports (faster this way) 
            }
            boolean fFoundCoumpoundIndex = false, fFoundCorrectCoumpoundIndex = false, fFoundStartSiteIndex = false;
            MongoCursor<Document> indexCursor = coll.listIndexes().cursor();
            while (indexCursor.hasNext()) {
                Document doc = (Document) indexCursor.next();
                Document keyDoc = ((Document) doc.get("key"));
                Set<String> keyIndex = (Set<String>) keyDoc.keySet();
                if (keyIndex.size() == 1) {
                    if ((rpPath + ReferencePosition.FIELDNAME_START_SITE).equals(keyIndex.iterator().next())) {
                        fFoundStartSiteIndex = true;
                    }
                } else if (keyIndex.size() == 2) {	// compound index
                    String[] compoundIndexItems = keyIndex.toArray(new String[2]);
                    if (compoundIndexItems[0].equals(rpPath + ReferencePosition.FIELDNAME_SEQUENCE) && compoundIndexItems[1].equals(rpPath + ReferencePosition.FIELDNAME_START_SITE)) {
                        fFoundCoumpoundIndex = true;
                        Document collation = (Document) doc.get("collation");
                        fFoundCorrectCoumpoundIndex = collation != null && "en_US".equals(collation.get("locale")) && Boolean.TRUE.equals(collation.get("numericOrdering"));
                    }
                }
            }
            if (!fFoundStartSiteIndex) {
                Thread ssIndexCreationThread = new Thread() {
                    public void run() {
                        LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Creating index " + ssIndexKeys + " on collection " + coll.getNamespace());
                        coll.createIndex(ssIndexKeys);
                    }
                };
                ssIndexCreationThread.start();
                nResult++;
            }

            if (!fFoundCoumpoundIndex || (fFoundCoumpoundIndex && !fFoundCorrectCoumpoundIndex)) {
                final MongoCollection<Document> collToDropCompoundIndexOn = fFoundCoumpoundIndex ? coll : null;
                Thread ssIndexCreationThread = new Thread() {
                    public void run() {
                        if (collToDropCompoundIndexOn != null) {
                            LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Dropping wrong index " + coumpoundIndexKeys + " on collection " + collToDropCompoundIndexOn.getNamespace());
                            collToDropCompoundIndexOn.dropIndex(coumpoundIndexKeys);
                        }

                        LOG.log(fIsTmpColl ? Level.DEBUG : Level.INFO, "Creating index " + coumpoundIndexKeys + " on collection " + coll.getNamespace());
                        coll.createIndex(coumpoundIndexKeys, new IndexOptions().collation(IExportHandler.collationObj));
                    }
                };
                ssIndexCreationThread.start();

                nResult++;

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
     * @param projectIdToReturnedRunFieldListMap the project id to returned run
     * field list map
     * @param variantIdListToRestrictTo the variant id list to restrict to
     * @param sort the sort
     * @return the sample genotypes
     * @throws Exception the exception
     */
    private static LinkedHashMap<VariantData, Collection<VariantRunData>> getSampleGenotypes(MongoTemplate mongoTemplate, ArrayList<String> variantFieldsToReturn, HashMap<Integer, ArrayList<String>> projectIdToReturnedRunFieldListMap, List<Object> variantIdListToRestrictTo, Sort sort) throws Exception {
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
    public static LinkedHashMap<VariantData, Collection<VariantRunData>> getSampleGenotypes(MongoTemplate mongoTemplate, Collection<GenotypingSample> samples, List<Object> variantIdListToRestrictTo, boolean fReturnVariantTypes, Sort sort) throws Exception {
        ArrayList<String> variantFieldsToReturn = new ArrayList<String>();
        variantFieldsToReturn.add(VariantData.FIELDNAME_KNOWN_ALLELES);
        variantFieldsToReturn.add(VariantData.FIELDNAME_REFERENCE_POSITION);
        if (fReturnVariantTypes) {
            variantFieldsToReturn.add(VariantData.FIELDNAME_TYPE);
        }

        HashMap<Integer /*project id*/, ArrayList<String>> projectIdToReturnedRunFieldListMap = new HashMap<Integer, ArrayList<String>>();
        for (GenotypingSample sample : samples) {
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

    public static Set<String> getProjectIndividuals(String sModule, int projId) {
        return getSamplesByIndividualForProject(sModule, projId, null).keySet();
    }

    /**
     * Gets the individuals from samples.
     *
     * @param sModule the module
     * @param sampleIDs the sample ids
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

    public static TreeMap<String /*individual*/, ArrayList<GenotypingSample>> getSamplesByIndividualForProject(final String sModule, final int projId, final Collection<String> individuals) {
        TreeMap<String /*individual*/, ArrayList<GenotypingSample>> result = new TreeMap<>();
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        Criteria crit = Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).is(projId);
        if (individuals != null && individuals.size() > 0) {
            crit.andOperator(Criteria.where(GenotypingSample.FIELDNAME_INDIVIDUAL).in(individuals));
        }
        Query q = new Query(crit);
//		q.with(new Sort(Sort.Direction.ASC, GenotypingSample.SampleId.FIELDNAME_INDIVIDUAL));
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

    public static ArrayList<GenotypingSample> getSamplesForProject(final String sModule, final int projId, final Collection<String> individuals) {
        ArrayList<GenotypingSample> result = new ArrayList<>();
        for (ArrayList<GenotypingSample> sampleList : getSamplesByIndividualForProject(sModule, projId, individuals).values()) {
            result.addAll(sampleList);
        }
        return result;
    }

    /**
     * Gets individuals' populations.
     *
     * @param sModule the module
     * @param individuals the individual IDs
     * @return the individual ID to population map
     */
    public static Map<String, String> getIndividualPopulations(final String sModule, final Collection<String> individuals) {
        Query query = new Query(new Criteria().andOperator(Criteria.where("_id").in(individuals), Criteria.where(Individual.FIELDNAME_POPULATION).ne(null)));
        query.fields().include(Individual.FIELDNAME_POPULATION);
        return MongoTemplateManager.get(sModule).find(query, Individual.class).stream().collect(Collectors.toMap(ind -> ind.getId(), ind -> ind.getPopulation()));
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
     * This method is not static because it requires access to a HttpSession
     * which we get from a ObjectFactory<HttpSession> that we couln't autowire
     * from a static getter
     *
     * @param module the database name (mandatory)
     * @param sCurrentUser username for whom to get custom metadata (optional)
     * @param projIDs a list of project IDs (optional)
     * @param indIDs a list of individual IDs (optional)
     * @return Individual IDs mapped to Individual objects with static metada +
     * custom metadata (if available). If indIDs is specified the list is
     * restricted by it, otherwise if projIDs is specified the list is
     * restricted by it, otherwise all database Individuals are returned
     */
    public LinkedHashMap<String, Individual> loadIndividualsWithAllMetadata(String module,/* HttpSession session, */ String sCurrentUser, Collection<Integer> projIDs, Collection<String> indIDs) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);

        // build the initial list of Individual objects
        if (indIDs == null) {
            indIDs = mongoTemplate.findDistinct(projIDs == null || projIDs.isEmpty() ? new Query() : new Query(Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).in(projIDs)), GenotypingSample.FIELDNAME_INDIVIDUAL, GenotypingSample.class, String.class);
        }
        Query q = new Query(Criteria.where("_id").in(indIDs));
        q.with(Sort.by(Sort.Direction.ASC, "_id"));
        Map<String, Individual> indMap = mongoTemplate.find(q, Individual.class).stream().collect(Collectors.toMap(Individual::getId, ind -> ind));
        LinkedHashMap<String, Individual> result = new LinkedHashMap<>();	// this one will be sorted according to the provided list
        for (String indId : indIDs) {
            result.put(indId, indMap.get(indId));
        }

        boolean fGrabSessionAttributesFromThread = SessionAttributeAwareExportThread.class.isAssignableFrom(Thread.currentThread().getClass());
        LinkedHashMap<String, LinkedHashMap<String, Object>> sessionMetaData = (LinkedHashMap<String, LinkedHashMap<String, Object>>) (fGrabSessionAttributesFromThread ? ((SessionAttributeAwareExportThread) Thread.currentThread()).getSessionAttributes().get("individuals_metadata_" + module) : httpSessionFactory.getObject().getAttribute("individuals_metadata_" + module));
        if (sCurrentUser != null) {	// merge with custom metadata if available
            if ("anonymousUser".equals(sCurrentUser) && sessionMetaData != null) {
                for (String indId : indIDs) {
                    LinkedHashMap<String, Object> indSessionMetadata = sessionMetaData.get(indId);
                    if (indSessionMetadata != null && !indSessionMetadata.isEmpty()) {
                        result.get(indId).getAdditionalInfo().putAll(indSessionMetadata);
                    }
                }
            } else {
                for (CustomIndividualMetadata cimd : mongoTemplate.find(new Query(new Criteria().andOperator(Criteria.where("_id." + CustomIndividualMetadataId.FIELDNAME_USER).is(sCurrentUser), Criteria.where("_id." + CustomIndividualMetadataId.FIELDNAME_INDIVIDUAL_ID).in(indIDs))), CustomIndividualMetadata.class)) {
                    if (cimd.getAdditionalInfo() != null && !cimd.getAdditionalInfo().isEmpty()) {
                        result.get(cimd.getId().getIndividualId()).getAdditionalInfo().putAll(cimd.getAdditionalInfo());
                    }
                }
            }
        }
        return result;
    }
}
