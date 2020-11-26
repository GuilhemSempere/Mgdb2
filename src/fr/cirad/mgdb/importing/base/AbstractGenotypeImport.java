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
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

public class AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(AbstractGenotypeImport.class);

	private boolean m_fAllowDbDropIfNoGenotypingData = true;

	/** String representing nucleotides considered as valid */
	protected static HashSet<String> validNucleotides = new HashSet<>(Arrays.asList(new String[] {"a", "A", "t", "T", "g", "G", "c", "C"}));
	
	protected static HashMap<String /*module*/, String /*project*/> currentlyImportedProjects = new HashMap<>();
	
	public static ArrayList<String> getIdentificationStrings(String sType, String sSeq, Long nStartPos, Collection<String> idAndSynonyms) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();
		
		if (idAndSynonyms != null)
			for (String name : idAndSynonyms)
				result.add(name.toUpperCase());

		if (sSeq != null && nStartPos != null)
			result.add(sType + "¤" + sSeq + "¤" + nStartPos);

		if (result.size() == 0)
			throw new Exception("Not enough info provided to build identification strings");
		
		return result;
	}
	
	public static String getCurrentlyImportedProjectForModule(String sModule)
	{
		return currentlyImportedProjects.get(sModule);
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
		long variantCount = mongoTemplate.count(null, VariantData.class);
        if (variantCount > 0)
        {	// there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
            long beforeReadingAllVariants = System.currentTimeMillis();
            Query query = new Query();
            query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
            DBCursor variantIterator = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(query.getQueryObject(), query.getFieldsObject());
			while (variantIterator.hasNext())
			{
				DBObject vd = variantIterator.next();
				String variantIdAsString = vd.get("_id").toString();
				boolean fGotChrPos = vd.get(VariantData.FIELDNAME_REFERENCE_POSITION) != null;
				ArrayList<String> idAndSynonyms = new ArrayList<>();
				if (fIncludeRandomObjectIDs || !MgdbDao.idLooksGenerated(variantIdAsString))	// most of the time we avoid taking into account randomly generated IDs
					idAndSynonyms.add(variantIdAsString);
				DBObject synonymsByType = (DBObject) vd.get(VariantData.FIELDNAME_SYNONYMS);
				if (synonymsByType != null)
					for (String synonymType : synonymsByType.keySet())
						for (Object syn : (BasicDBList) synonymsByType.get(synonymType))
							idAndSynonyms.add((String) syn.toString());

				for (String variantDescForPos : getIdentificationStrings((String) vd.get(VariantData.FIELDNAME_TYPE), !fGotChrPos ? null : (String) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE), !fGotChrPos ? null : (long) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE), idAndSynonyms))
				{
					if (existingVariantIDs.containsKey(variantDescForPos) && !variantIdAsString.startsWith("*"))
			        	throw new Exception("This database seems to contain duplicate variants. Importing additional data will not be supported until this problem is fixed.");

					existingVariantIDs.put(variantDescForPos, vd.get("_id").toString());
				}
			}
            LOG.info(mongoTemplate.count(null, VariantData.class) + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s");
        }
        return existingVariantIDs;
	}
	
	static public boolean doesDatabaseSupportImportingUnknownVariants(String sModule)
	{
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		String firstId = null, lastId = null;
		Query query = new Query(Criteria.where("_id").not().regex("^\\*.*"));
        query.with(new Sort("_id"));
        query.fields().include("_id");
        VariantData firstVariant = mongoTemplate.findOne(query, VariantData.class);
		if (firstVariant != null)
			firstId = firstVariant.getId().toString();
		query.with(new Sort(Sort.Direction.DESC, "_id"));
		VariantData lastVariant = mongoTemplate.findOne(query, VariantData.class);
		if (lastVariant != null)
			lastId = lastVariant.getId().toString();

		boolean fLooksLikePreprocessedVariantList = firstId != null && firstId.endsWith("001") && mongoTemplate.count(new Query(Criteria.where("_id").not().regex("^\\*?" + StringUtils.getCommonPrefix(new String[] {firstId, lastId}) + ".*")), VariantData.class) == 0;
//		LOG.debug("Database " + sModule + " does " + (fLooksLikePreprocessedVariantList ? "not " : "") + "support importing unknown variants");
		return !fLooksLikePreprocessedVariantList;
	}	

    public void persistVariantsAndGenotypes(HashMap<String, String> existingVariantIDs, MongoTemplate mongoTemplate, Collection<VariantData> unsavedVariants, Collection<VariantRunData> unsavedRuns)
    {
        if (existingVariantIDs.size() == 0) {	// we benefit from the fact that it's the first variant import into this database to use bulk insert which is much faster
        	mongoTemplate.insert(unsavedVariants, VariantData.class);
        	mongoTemplate.insert(unsavedRuns, VariantRunData.class);
        }
        else
        {
            for (VariantData vd : unsavedVariants)
            	mongoTemplate.save(vd);
            for (VariantRunData run : unsavedRuns)
            	mongoTemplate.save(run);
        }  
    }
    
    protected void cleanupBeforeImport(MongoTemplate mongoTemplate, String sModule, GenotypingProject project, int importMode, String sRun) {
        if (importMode == 2)
            mongoTemplate.getDb().dropDatabase(); // drop database before importing
        else if (project != null)
        {
			if (importMode == 1 || (project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
			{	// empty project data before importing
				WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId())), DBVCFHeader.class);
				if (wr.getN() > 0)
					LOG.info(wr.getN() + " records removed from vcf_header");
				wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId())), VariantRunData.class);
				if (wr.getN() > 0)
					LOG.info(wr.getN() + " variantRunData records removed while cleaning up project " + project.getId() + "'s data");
				wr = mongoTemplate.remove(new Query(Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).is(project.getId())), GenotypingSample.class);
				if (wr.getN() > 0)
					LOG.info(wr.getN() + " samples were removed while cleaning up project " + project.getId() + "'s data");
				mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);
			}
			else
			{	// empty run data before importing
                WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId()).and("_id." + VcfHeaderId.FIELDNAME_RUN).is(sRun)), DBVCFHeader.class);
                if (wr.getN() > 0)
                	LOG.info(wr.getN() + " records removed from vcf_header");
                if (project.getRuns().contains(sRun))
                {
                	LOG.info("Cleaning up existing run's data");
                    List<Criteria> crits = new ArrayList<>();
                    crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()));
                    crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME).is(sRun));
                    crits.add(Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).exists(true));
                    wr = mongoTemplate.remove(new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()]))), VariantRunData.class);
                    if (wr.getN() > 0)
                    	LOG.info(wr.getN() + " variantRunData records removed while cleaning up project " + project.getId() + "'s data");
    				wr = mongoTemplate.remove(new Query(new Criteria().andOperator(Criteria.where(GenotypingSample.FIELDNAME_PROJECT_ID).is(project.getId()), Criteria.where(GenotypingSample.FIELDNAME_RUN).is(sRun))), GenotypingSample.class);
    				if (wr.getN() > 0)
    					LOG.info(wr.getN() + " samples were removed while cleaning up project " + project.getId() + "'s data");

                }
            }
			if (mongoTemplate.count(null, VariantRunData.class) == 0 && m_fAllowDbDropIfNoGenotypingData && doesDatabaseSupportImportingUnknownVariants(sModule))
                mongoTemplate.getDb().dropDatabase();	// if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
        }
	}
    
	public boolean isAllowedToDropDbIfNoGenotypingData() {
		return m_fAllowDbDropIfNoGenotypingData;
	}

	public void allowDbDropIfNoGenotypingData(boolean fAllowDbDropIfNoGenotypingData) {
		this.m_fAllowDbDropIfNoGenotypingData = fAllowDbDropIfNoGenotypingData;
	}
}
