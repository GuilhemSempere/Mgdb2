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
package fr.cirad.mgdb.importing.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.DeleteResult;

import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class AbstractGenotypeImport {

	private static final Logger LOG = Logger.getLogger(AbstractGenotypeImport.class);

	protected static final int nMaxChunkSize = 20000;

	private boolean m_fAllowDbDropIfNoGenotypingData = true;

	public static ArrayList<String> getIdentificationStrings(String sType, String sSeq, Long nStartPos, Collection<String> idAndSynonyms) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();

		if (idAndSynonyms != null)
			result.addAll(idAndSynonyms.stream().map(s -> s.toUpperCase()).collect(Collectors.toList()));

		if (sSeq != null && nStartPos != null)
			result.add(new StringBuilder(sType).append("¤").append(sSeq).append("¤").append(nStartPos).toString());

		if (result.size() == 0)
			throw new Exception("Not enough info provided to build identification strings");

		return result;
	}


//	public static void buildSynonymMappings(MongoTemplate mongoTemplate) throws Exception
//	{
//		DBCollection collection = mongoTemplate.getCollection(COLLECTION_NAME_SYNONYM_MAPPINGS);
//		collection.drop();
//
//		long variantCount = mongoTemplate.count(null, VariantData.class);
//        if (variantCount > 0)
//        {	// there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
//            long beforeReadingAllVariants = System.currentTimeMillis();
//            Query query = new Query();
//            query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
//            DBCursor variantIterator = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(query.getQueryObject(), query.getFieldsObject());
//            int i = 0, nDups = 0;
//			while (variantIterator.hasNext())
//			{
//				DBObject vd = variantIterator.next();
//				boolean fGotChrPos = vd.get(VariantData.FIELDNAME_REFERENCE_POSITION) != null;
//				ArrayList<String> idAndSynonyms = new ArrayList<>();
//				idAndSynonyms.add(vd.get("_id").toString());
//				DBObject synonymsByType = (DBObject) vd.get(VariantData.FIELDNAME_SYNONYMS);
//				for (String synonymType : synonymsByType.keySet())
//					for (Object syn : (BasicDBList) synonymsByType.get(synonymType))
//						idAndSynonyms.add((String) syn.toString());
//
//				for (String variantDescForPos : getIdentificationStrings((String) vd.get(VariantData.FIELDNAME_TYPE), !fGotChrPos ? null : (String) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE), !fGotChrPos ? null : (long) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE), idAndSynonyms))
//				{
//					BasicDBObject synonymMapping = new BasicDBObject("_id", variantDescForPos);
//					synonymMapping.put("id", vd.get("_id"));
//					try
//					{
//						collection.insert(synonymMapping);
//					}
//					catch (DuplicateKeyException dke)
//					{
//						nDups++;
//						LOG.error(dke.getMessage());
//					}
//				}
//
//				float nProgressPercentage = ++i * 100f / variantCount;
//				if (nProgressPercentage % 10 == 0)
//					LOG.debug("buildSynonymMappings: " + nProgressPercentage + "%");
//			}
//            LOG.info(mongoTemplate.count(null, VariantData.class) + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s");
//            if (nDups > 0)
//            	LOG.warn(nDups + " duplicates found in database " + mongoTemplate.getDb().getName());
//        }
//	}

	protected static HashMap<String, String> buildSynonymToIdMapForExistingVariants(MongoTemplate mongoTemplate, boolean fIncludeRandomObjectIDs) throws Exception
	{
        HashMap<String, String> existingVariantIDs = new HashMap<>();
		long variantCount = Helper.estimDocCount(mongoTemplate,VariantData.class);
        if (variantCount > 0)
        {	// there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
            long beforeReadingAllVariants = System.currentTimeMillis();
            Query query = new Query();
            query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
            MongoCursor<Document> variantIterator = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(query.getQueryObject()).projection(query.getFieldsObject()).iterator();
			while (variantIterator.hasNext())
			{
				Document vd = variantIterator.next();
				String variantIdAsString = vd.get("_id").toString();
				boolean fGotChrPos = vd.get(VariantData.FIELDNAME_REFERENCE_POSITION) != null;
				ArrayList<String> idAndSynonyms = new ArrayList<>();
				if (fIncludeRandomObjectIDs || !MgdbDao.idLooksGenerated(variantIdAsString))	// most of the time we avoid taking into account randomly generated IDs
					idAndSynonyms.add(variantIdAsString);
				Document synonymsByType = (Document) vd.get(VariantData.FIELDNAME_SYNONYMS);
				if (synonymsByType != null)
					for (String synonymType : synonymsByType.keySet())
						for (Object syn : (List) synonymsByType.get(synonymType))
							idAndSynonyms.add((String) syn.toString());

				for (String variantDescForPos : getIdentificationStrings((String) vd.get(VariantData.FIELDNAME_TYPE), !fGotChrPos ? null : (String) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, ";"), !fGotChrPos ? null : (long) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, ";"), idAndSynonyms))
				{
					if (existingVariantIDs.containsKey(variantDescForPos) && !variantIdAsString.startsWith("*"))
			        	throw new Exception("This database seems to contain duplicate variants (check " + variantDescForPos.replaceAll("¤", ":") + "). Importing additional data will not be supported until this problem is fixed.");

					existingVariantIDs.put(variantDescForPos, vd.get("_id").toString());
				}
			}
            LOG.info(Helper.estimDocCount(mongoTemplate,VariantData.class) + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s");
        }
        return existingVariantIDs;
	}

	static public boolean doesDatabaseSupportImportingUnknownVariants(String sModule)
	{
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		String firstId = null, lastId = null;
		Query query = new Query(Criteria.where("_id").not().regex("^\\*.*"));
		query.with(Sort.by(Arrays.asList(new Sort.Order(Sort.Direction.ASC, "_id"))));
        query.fields().include("_id");
        VariantData firstVariant = mongoTemplate.findOne(query, VariantData.class);
		if (firstVariant != null)
			firstId = firstVariant.getId().toString();
		query.with(Sort.by(Arrays.asList(new Sort.Order(Sort.Direction.DESC, "_id"))));
		VariantData lastVariant = mongoTemplate.findOne(query, VariantData.class);
		if (lastVariant != null)
			lastId = lastVariant.getId().toString();

		boolean fLooksLikePreprocessedVariantList = firstId != null && firstId.endsWith("001") && mongoTemplate.count(new Query(Criteria.where("_id").not().regex("^\\*?" + StringUtils.getCommonPrefix(new String[] {firstId, lastId}) + ".*")), VariantData.class) == 0;
//		LOG.debug("Database " + sModule + " does " + (fLooksLikePreprocessedVariantList ? "not " : "") + "support importing unknown variants");
		return !fLooksLikePreprocessedVariantList;
	}

	protected void saveChunk(final Collection<VariantData> unsavedVariants, final Collection<VariantRunData> unsavedRuns, HashMap<String, String> existingVariantIDs, MongoTemplate finalMongoTemplate, ProgressIndicator progress, ExecutorService saveService) throws InterruptedException {
        Thread insertionThread = new Thread() {
            @Override
            public void run() {
        		try {
					persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), finalMongoTemplate, unsavedVariants, unsavedRuns);
        		} catch (InterruptedException e) {
					progress.setError(e.getMessage());
					LOG.error(e);
				}
            }
        };

        saveService.execute(insertionThread);
	}

	protected int saveServiceQueueLength(int nConcurrentThreads) {
		return nConcurrentThreads * 6;
	}

	protected int saveServiceThreads(int nConcurrentThreads) {
		return nConcurrentThreads * 3;
	}

    public void persistVariantsAndGenotypes(boolean fDBAlreadyContainsVariants, MongoTemplate mongoTemplate, Collection<VariantData> unsavedVariants, Collection<VariantRunData> unsavedRuns) throws InterruptedException
    {
//    	long b4 = System.currentTimeMillis();
		Thread vdAsyncThread = new Thread() {	// using 2 threads is faster when calling save, but slower when calling insert
			public void run() {
				if (!fDBAlreadyContainsVariants) {	// we benefit from the fact that it's the first variant import into this database to use bulk insert which is much faster
					mongoTemplate.insert(unsavedVariants, VariantData.class);
				} else {
			    	for (VariantData vd : unsavedVariants) {
			        	try {
			        		mongoTemplate.save(vd);
			        	}
						catch (OptimisticLockingFailureException olfe) {
							mongoTemplate.save(vd);	// try again
						}
			    	}
				}
			}
		};
		vdAsyncThread.start();

//		long t1 = System.currentTimeMillis() - b4;
//		b4 = System.currentTimeMillis();

		List<VariantRunData> syncList = new ArrayList<>(), asyncList = new ArrayList<>();
		int i = 0;
		for (VariantRunData vrd : unsavedRuns)
			(i++ < unsavedRuns.size() / 2 ? syncList : asyncList).add(vrd);

		try {
			Thread vrdAsyncThread = new Thread() {
				public void run() {
			    	mongoTemplate.insert(asyncList, VariantRunData.class);	// this should always work but fails when a same variant is provided several times (using different synonyms)
				}
			};
			vrdAsyncThread.start();
	    	mongoTemplate.insert(syncList, VariantRunData.class);	// this should always work but fails when a same variant is provided several times (using different synonyms)
			vrdAsyncThread.join();
		}
		catch (DuplicateKeyException dke)
		{
			LOG.info("Persisting runs using save() because of synonym variants: " + dke.getMessage());
			Thread vrdAsyncThread = new Thread() {	// using 2 threads is faster when calling save, but slower when calling insert
				public void run() {
			    	asyncList.stream().forEach(vrd -> mongoTemplate.save(vrd));
				}
			};
			vrdAsyncThread.start();
    		syncList.stream().forEach(vrd -> mongoTemplate.save(vrd));
			vrdAsyncThread.join();
		}

		vdAsyncThread.join();
//		System.err.println("VD: " + t1 + " / VRD: " + (System.currentTimeMillis() - b4));
    }

    protected void cleanupBeforeImport(MongoTemplate mongoTemplate, String sModule, GenotypingProject project, int importMode, String sRun) {
        if (importMode == 2)
            mongoTemplate.getDb().drop(); // drop database before importing
        else if (project != null)
        {
			if (importMode == 1 || (project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
			{	// empty project data before importing
				DeleteResult dr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId())), DBVCFHeader.class);
				if (dr.getDeletedCount() > 0)
					LOG.info(dr.getDeletedCount() + " records removed from vcf_header");
				dr = mongoTemplate.remove(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId())), VariantRunData.class);
				if (dr.getDeletedCount() > 0)
					LOG.info(dr.getDeletedCount() + " variantRunData records removed while cleaning up project " + project.getId() + "'s data");
				dr = mongoTemplate.remove(new Query(Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).is(project.getId())), GenotypingSample.class);
				if (dr.getDeletedCount() > 0)
					LOG.info(dr.getDeletedCount() + " samples were removed while cleaning up project " + project.getId() + "'s data");
				mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);
			}
			else
			{	// empty run data before importing
				DeleteResult dr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId()).and("_id." + VcfHeaderId.FIELDNAME_RUN).is(sRun)), DBVCFHeader.class);
                if (dr.getDeletedCount() > 0)
                	LOG.info(dr.getDeletedCount() + " records removed from vcf_header");
                if (project.getRuns().contains(sRun))
                {
                	LOG.info("Cleaning up existing run's data");
                    List<Criteria> crits = new ArrayList<>();
                    crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()));
                    crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME).is(sRun));
                    crits.add(Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).exists(true));
                    dr = mongoTemplate.remove(new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()]))), VariantRunData.class);
                    if (dr.getDeletedCount() > 0)
                    	LOG.info(dr.getDeletedCount() + " variantRunData records removed while cleaning up project " + project.getId() + "'s data");
    				dr = mongoTemplate.remove(new Query(new Criteria().andOperator(Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).is(project.getId()), Criteria.where(GenotypingSample.FIELDNAME_RUN).is(sRun))), GenotypingSample.class);
    				if (dr.getDeletedCount() > 0)
    					LOG.info(dr.getDeletedCount() + " samples were removed while cleaning up project " + project.getId() + "'s data");

                }
            }
			if (Helper.estimDocCount(mongoTemplate,VariantRunData.class) == 0 && m_fAllowDbDropIfNoGenotypingData && doesDatabaseSupportImportingUnknownVariants(sModule))
                mongoTemplate.getDb().drop();	// if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
        }
	}

	public boolean isAllowedToDropDbIfNoGenotypingData() {
		return m_fAllowDbDropIfNoGenotypingData;
	}

	public void allowDbDropIfNoGenotypingData(boolean fAllowDbDropIfNoGenotypingData) {
		this.m_fAllowDbDropIfNoGenotypingData = fAllowDbDropIfNoGenotypingData;
	}

	/**
	 * Code copied from htsjdk.variant.variantcontext.VariantContext (Copyright The Broad Institute) and adapted for convenience,
	 */
    public static Type determineType(Collection<Allele> alleles) {
        switch ( alleles.size() ) {
            case 0:
                throw new IllegalStateException("Unexpected error: requested type of VariantContext with no alleles!");
            case 1:
                // note that this doesn't require a reference allele.  You can be monomorphic independent of having a reference allele
                return Type.NO_VARIATION;
            default:
                return determinePolymorphicType(alleles);
        }
    }

    /**
     * Code copied from htsjdk.variant.variantcontext.VariantContext (Copyright The Broad Institute) and adapted for convenience,
     */
    public static Type determinePolymorphicType(Collection<Allele> alleles) {
        Type type = null;
        Allele refAllele = alleles.iterator().next();

        // do a pairwise comparison of all alleles against the reference allele
        for ( Allele allele : alleles ) {
            if ( allele == refAllele )
                continue;

            // find the type of this allele relative to the reference
            Type biallelicType = typeOfBiallelicVariant(refAllele, allele);

            // for the first alternate allele, set the type to be that one
            if ( type == null ) {
                type = biallelicType;
            }
            // if the type of this allele is different from that of a previous one, assign it the MIXED type and quit
            else if ( biallelicType != type ) {
                return Type.MIXED;
            }
        }
        return type;
    }

    /**
     * Code copied from htsjdk.variant.variantcontext.VariantContext (Copyright The Broad Institute) and adapted for convenience,
     */
    public static Type typeOfBiallelicVariant(Allele ref, Allele allele) {
        if ( ref.isSymbolic() )
            throw new IllegalStateException("Unexpected error: encountered a record with a symbolic reference allele");

        if ( allele.isSymbolic() )
            return Type.SYMBOLIC;

        if ( ref.length() == allele.length() ) {
            if ( allele.length() == 1 )
                return Type.SNP;
            else
                return Type.MNP;
        }

        // Important note: previously we were checking that one allele is the prefix of the other.  However, that's not an
        // appropriate check as can be seen from the following example:
        // REF = CTTA and ALT = C,CT,CA
        // This should be assigned the INDEL type but was being marked as a MIXED type because of the prefix check.
        // In truth, it should be absolutely impossible to return a MIXED type from this method because it simply
        // performs a pairwise comparison of a single alternate allele against the reference allele (whereas the MIXED type
        // is reserved for cases of multiple alternate alleles of different types).  Therefore, if we've reached this point
        // in the code (so we're not a SNP, MNP, or symbolic allele), we absolutely must be an INDEL.

        return Type.INDEL;

        // old incorrect logic:
        // if (oneIsPrefixOfOther(ref, allele))
        //     return Type.INDEL;
        // else
        //     return Type.MIXED;
    }
}
