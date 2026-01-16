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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.brapi.v2.model.Sample;
import org.bson.Document;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.UpdateResult;

import fr.cirad.mgdb.importing.IndividualMetadataImport;
import fr.cirad.mgdb.importing.parameters.ImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;
import jhi.brapi.api.samples.BrapiSample;

public abstract class AbstractGenotypeImport<T extends ImportParameters> {
    protected String m_processID;

	private static final Logger LOG = Logger.getLogger(AbstractGenotypeImport.class);

	protected static final int nMaxChunkSize = 20000;

	private boolean m_fAllowDbDropIfNoGenotypingData = true;
	public boolean m_fCloseContextAfterImport = false;
    public boolean m_fAllowNewAssembly = true;
	private boolean m_fSamplesPersisted = false;

	protected Map<String /* individual or sample name */, GenotypingSample> m_providedIdToSampleMap = null;
    protected Map<String /* individual or sample name */, Callset> m_providedIdToCallsetMap = null;
    protected List<Callset> m_callsets = new ArrayList<>();
	
	protected String brapiEndPointUriForNamingIndividuals;
	protected String brapiEndPointTokenForNamingIndividuals;
	
	private HashMap<String, String> preloadedSampleToIndividualMap = null;
	
	public void setBrapiEndPointForNamingIndividuals(String brapiEndPointUri, String brapiEndPointToken) {
		brapiEndPointUriForNamingIndividuals = !brapiEndPointUri.endsWith("/") ? brapiEndPointUri + "/" : brapiEndPointUri;
		brapiEndPointTokenForNamingIndividuals = "".equals(brapiEndPointToken) ? null : brapiEndPointToken;
	}

	protected void setSamplesPersisted(boolean m_fSamplesPersisted) {
		this.m_fSamplesPersisted = m_fSamplesPersisted;
	}

	protected void attemptPreloadingIndividuals(Collection<String> sampleDbIds, ProgressIndicator progress) throws Exception {
		if (brapiEndPointUriForNamingIndividuals == null || preloadedSampleToIndividualMap != null)
			return;

		try {
			if (progress != null)
				progress.setProgressDescription("Getting germplasmDbId for " + sampleDbIds.size() + " samples from " + brapiEndPointUriForNamingIndividuals);
			
			preloadedSampleToIndividualMap = new HashMap<>();
			if (brapiEndPointUriForNamingIndividuals.endsWith("/v1/"))
				for (BrapiSample sp : IndividualMetadataImport.readBrapiV1Samples(brapiEndPointUriForNamingIndividuals, brapiEndPointTokenForNamingIndividuals, sampleDbIds, null))
					preloadedSampleToIndividualMap.put(sp.getSampleDbId(), sp.getGermplasmDbId());
			else
				for (Sample sp : IndividualMetadataImport.readBrapiV2Samples(brapiEndPointUriForNamingIndividuals, brapiEndPointTokenForNamingIndividuals, sampleDbIds, null))
					preloadedSampleToIndividualMap.put(sp.getSampleDbId(), sp.getGermplasmDbId());
		}
		finally {
			if (progress != null)
				progress.setProgressDescription(null);
		}
	}

	protected String determineIndividualNameAccountingForBrapiRelationships(Map<String, String> sampleToIndividualMap, String sampleDbId, ProgressIndicator progress) throws Exception {
		String indName = preloadedSampleToIndividualMap == null ? null : preloadedSampleToIndividualMap.get(sampleDbId);
		if (indName == null && brapiEndPointUriForNamingIndividuals != null) {
			if (progress != null)
				progress.setProgressDescription("Getting germplasmDbId for sample " + sampleDbId + " from " + brapiEndPointUriForNamingIndividuals);
			try {
				if (brapiEndPointUriForNamingIndividuals.endsWith("/v1/")) {
					List<BrapiSample> samples = IndividualMetadataImport.readBrapiV1Samples(brapiEndPointUriForNamingIndividuals, brapiEndPointTokenForNamingIndividuals, Arrays.asList(sampleDbId), null);
					if (samples.size() != 1)
						LOG.error("Unable to find individual name from BrAPI endpoint " + brapiEndPointUriForNamingIndividuals + " for sample " + sampleDbId);
					else
						indName = samples.get(0).getGermplasmDbId();
				}
				else {
					List<Sample> samples = IndividualMetadataImport.readBrapiV2Samples(brapiEndPointUriForNamingIndividuals, brapiEndPointTokenForNamingIndividuals, Arrays.asList(sampleDbId), null);
					if (samples.size() != 1)
						LOG.error("Unable to find individual name from BrAPI endpoint " + brapiEndPointUriForNamingIndividuals + " for sample " + sampleDbId);
					else
						indName = samples.get(0).getGermplasmDbId();
				}
			}
			finally {
				if (progress != null)
					progress.setProgressDescription(null);
			}
		}
		if (indName == null)	// may happen if we're not using BrAPI here or if we tried to use it but failed to find a matching entity
			indName = sampleToIndividualMap == null || sampleToIndividualMap.isEmpty() /*empty means no mapping file but sample names provided: individuals will be named same as samples*/ ? sampleDbId : sampleToIndividualMap.get(sampleDbId);
		return indName;
	}

	public static ArrayList<String> getIdentificationStrings(String sType, String sSeq, Long nStartPos, Collection<String> idAndSynonyms) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();

		if (idAndSynonyms != null)
			result.addAll(idAndSynonyms.stream().filter(s -> s != null && !s.isEmpty() && !s.equals(".")).map(s -> s.toUpperCase()).collect(Collectors.toList()));

		if (sSeq != null && nStartPos != null)
			result.add(new StringBuilder(sType).append("¤").append(sSeq).append("¤").append(nStartPos).toString());

		if (result.size() == 0)
			throw new Exception("Not enough info provided to build identification strings");

		return result;
	}
	
	static public HashMap<String, String> readSampleMappingFile(URL sampleMappingFileURL) throws Exception {
		if (sampleMappingFileURL == null)
			return null;

        HashMap<String, String> sampleToIndividualMap = new HashMap<>();
    	Scanner sampleMappingScanner = new Scanner(new File(sampleMappingFileURL.toURI()));
    	int nIndividualColPos = -1, nSampleColPos = -1;
    	while (sampleMappingScanner.hasNextLine()) {
    		String[] splitLine = sampleMappingScanner.nextLine().split("\t");
    		if (splitLine.length < 2)
    			continue;	// probably a blank line

    		if (nIndividualColPos == -1) {
    			nIndividualColPos = "individual".equals(splitLine[0].toLowerCase()) ? 0 : 1;
    			nSampleColPos = nIndividualColPos == 0 ? 1 : 0;
    		}
    		else
    			sampleToIndividualMap.put(splitLine[nSampleColPos], splitLine[nIndividualColPos]);
    	}
    	sampleMappingScanner.close();
		return sampleToIndividualMap;
	}

	public boolean haveSamplesBeenPersisted() {
		return m_fSamplesPersisted;
	}
	
	protected Assembly createAssemblyIfNeeded(MongoTemplate mongoTemplate, String assemblyName) throws Exception {
        Assembly assembly = null;
        if (!mongoTemplate.findDistinct(new Query(), GenotypingProject.FIELDNAME_SEQUENCES, GenotypingProject.class, String.class).isEmpty()) {	// working on an existing old-style (assembly-less) database
        	if (assemblyName != null)
        		throw new Exception("This database does not support assemblies.");
        }
        else {	// DB is either empty or new-style (assembly-aware)
        	assembly = mongoTemplate.findOne(new Query(Criteria.where(Assembly.FIELDNAME_NAME).is(assemblyName)), Assembly.class);
            if (assembly == null) {
            	if ("".equals(assemblyName) || m_fAllowNewAssembly) {
                    assembly = new Assembly(assemblyName == null || "".equals(assemblyName) ? 0 : AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(Assembly.class)));
                    assembly.setName(assemblyName);
                    mongoTemplate.save(assembly);
                }
                else
                    throw new Exception("Assembly \"" + assemblyName + "\" not found in database. Supported assemblies are " + StringUtils.join(mongoTemplate.findDistinct(Assembly.FIELDNAME_NAME, Assembly.class, String.class), ", "));
            }
        }
        return assembly;
	}
	
    protected static HashMap<String, String> buildSynonymToIdMapForExistingVariants(MongoTemplate mongoTemplate, boolean fIncludeRandomObjectIDs, Integer assemblyId) throws Exception
    {
        HashMap<String, String> existingVariantIDs = new HashMap<>();
        long variantCount = Helper.estimDocCount(mongoTemplate,VariantData.class);
        
        if (variantCount > 0)
        {   // there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
            long beforeReadingAllVariants = System.currentTimeMillis();
            Query query = new Query();
    		String refPosPath = assemblyId != null ? Assembly.getVariantRefPosPath(assemblyId) : null;
    		if (refPosPath != null)
    			query.fields().include("_id").include(refPosPath).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
            MongoCursor<Document> variantIterator = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(query.getQueryObject()).projection(query.getFieldsObject()).iterator();
            while (variantIterator.hasNext())
            {
                Document vd = variantIterator.next();
                String variantId = vd.getString("_id");
                ArrayList<String> idAndSynonyms = new ArrayList<>();
                if (fIncludeRandomObjectIDs || !MgdbDao.idLooksGenerated(variantId))    // most of the time we avoid taking into account randomly generated IDs
                    idAndSynonyms.add(variantId);
                Document synonymsByType = (Document) vd.get(VariantData.FIELDNAME_SYNONYMS);
                if (synonymsByType != null)
                    for (String synonymType : synonymsByType.keySet())
                        for (Object syn : (List) synonymsByType.get(synonymType))
                            idAndSynonyms.add((String) syn.toString());

                ArrayList<String> identificationStrings = getIdentificationStrings((String) vd.get(VariantData.FIELDNAME_TYPE), refPosPath == null ? null : (String) Helper.readPossiblyNestedField(vd, refPosPath + "." + ReferencePosition.FIELDNAME_SEQUENCE, ";", null), refPosPath == null ? null : (Long) Helper.readPossiblyNestedField(vd, refPosPath + "." + ReferencePosition.FIELDNAME_START_SITE, ";", null), idAndSynonyms);
                for (String variantDescForPos : identificationStrings) {
                    if (existingVariantIDs.containsKey(variantDescForPos) && !variantId.startsWith("*"))
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
        if (progress.getError() != null || progress.isAborted())
            return;

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
			AtomicReference<DuplicateKeyException> asyncException = new AtomicReference<>();
			Thread vrdAsyncThread = new Thread() {
				public void run() {
					try {
						mongoTemplate.insert(asyncList, VariantRunData.class);	// this should always work but fails when a same variant is provided several times (using different synonyms)
					}
					catch (DuplicateKeyException dke) {
						asyncException.set(dke);
					}
				}
			};
			vrdAsyncThread.start();
	    	mongoTemplate.insert(syncList, VariantRunData.class);	// this should always work but fails when a same variant is provided several times (using different synonyms)
			vrdAsyncThread.join();
			if (asyncException.get() != null)
				throw asyncException.get();
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

    protected void cleanupBeforeImport(MongoTemplate mongoTemplate, String sModule, GenotypingProject project, int importMode, String sRun) throws Exception {
        if (importMode == 2)
            mongoTemplate.getDb().drop(); // drop database before importing
        else if (project != null)
        {
        	boolean fAnythingChanged = false;
			if (importMode == 1 || (project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
				fAnythingChanged = MgdbDao.removeProjectAndRelatedRecords(sModule, project.getId());	// empty project data before importing
			else
				fAnythingChanged = MgdbDao.removeRunAndRelatedRecords(sModule, project.getId(), sRun, false);	// empty run data before importing

			if (fAnythingChanged)
				MongoTemplateManager.updateDatabaseLastModification(sModule);

			if (Helper.estimDocCount(mongoTemplate, VariantRunData.class) == 0 && m_fAllowDbDropIfNoGenotypingData && doesDatabaseSupportImportingUnknownVariants(sModule))
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

    public Integer importToMongo(T params) throws Exception {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"}); // better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)

        Integer createdProject = null;
        try {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(params.getModule());

            if (m_processID == null)
                m_processID = "IMPORT__" + params.getModule() + "__" + params.getProject() + "__" + params.getRun() + "__" + System.currentTimeMillis();

            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(params.getProject())), GenotypingProject.class);

            initReader(params);

            //The ploidy level can be given by the user or guessed from the data file
            Integer nPloidy = findPloidyLevel(mongoTemplate, params.getPloidy(), progress);

            if (params.getImportMode() == 0 && project != null && nPloidy != null && project.getPloidyLevel() != nPloidy)
                throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + params.getPloidy() + ") data!");

            MongoTemplateManager.lockProjectForWriting(params.getModule(), params.getProject());

            cleanupBeforeImport(mongoTemplate, params.getModule(), project, params.getImportMode(), params.getRun());

            if (project == null || params.getImportMode() > 0) {   // create it
                project = createProject(mongoTemplate, params.getProject(), params.getTechnology(), nPloidy == null ? 0 : nPloidy, progress);
                if (params.getImportMode() != 1)
                    createdProject = project.getId();
            }

            // specific part
            long count = doImport(params, mongoTemplate, project, progress, createdProject);

            if (!project.getRuns().contains(params.getRun()))
                project.getRuns().add(params.getRun());
            mongoTemplate.save(project);

            String importType = params.getClass().getSimpleName();
            if (importType.endsWith("Parameters")) {
                importType = importType.substring(0, importType.length() - "Parameters".length());
            }
            LOG.info(importType + " Import took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");

            return createdProject;

        } catch (Exception e) {
            LOG.error("Error", e);
            progress.setError(e.getMessage());
            return createdProject;
        } finally {
            closeResource();
            if (m_fCloseContextAfterImport)
            	MongoTemplateManager.closeApplicationContextIfOffline();
            MongoTemplateManager.unlockProjectForWriting(params.getModule(), params.getProject());
            if (progress.getError() == null && !progress.isAborted()) {
                progress.addStep("Preparing database for searches");
                progress.moveToNextStep();
                MgdbDao.prepareDatabaseForSearches(params.getModule());
            }
        }
    }

    protected abstract long doImport(T params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception;
    protected abstract void initReader(T params) throws Exception;
    protected abstract void closeResource() throws IOException;
    protected Integer findPloidyLevel(MongoTemplate mongoTemplate, Integer nPloidyParam, ProgressIndicator progress) throws Exception {
        return nPloidyParam;
    }

    protected GenotypingProject createProject(MongoTemplate mongoTemplate, String sProject, String sTechnology, Integer nPloidy, ProgressIndicator progress) throws IOException {
        GenotypingProject project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
        project.setName(sProject);
//                project.setOrigin(2 /* Sequencing */);
        project.setTechnology(sTechnology);
        project.setPloidyLevel(nPloidy);
        return project;
    }

    protected void createCallSetsSamplesIndividuals(Collection<String> biologicalMaterialIDs, MongoTemplate mongoTemplate, int projId, String sRun, Map<String, String> sampleToIndividualMap, ProgressIndicator progress) throws Exception {
    	progress.setProgressDescription("Creating / updating encountered biological entities...");
        m_providedIdToSampleMap = new HashMap<String /*individual*/, GenotypingSample>();
        m_providedIdToCallsetMap = new HashMap<String /*individual*/, Callset>();
        HashSet<Individual> indsToAdd = new HashSet<>();
        HashSet<GenotypingSample> samplesToAdd = new HashSet<>(), samplesToUpdate = new HashSet<>();
        boolean fDbAlreadyContainedIndividuals = mongoTemplate.findOne(new Query(), Individual.class) != null;
        boolean fDbAlreadyContainedSamples = mongoTemplate.findOne(new Query(), GenotypingSample.class) != null;       
        attemptPreloadingIndividuals(biologicalMaterialIDs, progress);

        for (String bioEntityID : biologicalMaterialIDs) {
        	GenotypingSample sample = null;
        	if (sampleToIndividualMap != null) {	// provided bio-entities are actually samples
        		if (fDbAlreadyContainedSamples) {
        			sample = mongoTemplate.findById(bioEntityID, GenotypingSample.class);
        			if (sample != null && !sampleToIndividualMap.isEmpty()) {	// the sample already exists in the DB, and a sample-to-individual mapping was provided for import: let's make sure individuals match
        				String sProvidedIndividualForThisSample = determineIndividualNameAccountingForBrapiRelationships(sampleToIndividualMap, bioEntityID, progress);
                    	if (sProvidedIndividualForThisSample != null && !sample.getIndividual().equals(sProvidedIndividualForThisSample)) {
        	                progress.setError("Sample " + bioEntityID + " already exists and is attached to individual " + sample.getIndividual() + ", not " + sProvidedIndividualForThisSample);
        	                return;
        	            }
        			}
        		}
        		if (sample == null) {
                    String sIndividual = determineIndividualNameAccountingForBrapiRelationships(sampleToIndividualMap, bioEntityID, progress);
                    if (sIndividual == null) {
                        progress.setError("Unable to determine individual for sample " + bioEntityID);
                        return;
                    }
        			sample = new GenotypingSample(bioEntityID, sIndividual);
                    samplesToAdd.add(sample);
        		}
        		else
        			samplesToUpdate.add(sample);
        	}
        	else {		// provided bio-entities are actually individuals
        		sample = new GenotypingSample(bioEntityID + "-" + projId + "-" + sRun, bioEntityID);
                samplesToAdd.add(sample);
        	}

            if (!fDbAlreadyContainedIndividuals || mongoTemplate.findById(sample.getIndividual(), Individual.class) == null)  // we don't have any population data so we don't need to update the Individual if it already exists
                indsToAdd.add(new Individual(sample.getIndividual()));

            m_providedIdToSampleMap.put(bioEntityID, sample);  // add a sample for this individual to the project
            int callsetId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(Callset.class));
            Callset cs = new Callset(callsetId, sample, projId, sRun);
            sample.getCallSets().add(cs);
            m_providedIdToCallsetMap.put(bioEntityID, cs);
        }

        insertNewCallSetsSamplesIndividuals(mongoTemplate, indsToAdd, samplesToAdd, samplesToUpdate);
        progress.setProgressDescription(null);
    }

    protected void insertNewCallSetsSamplesIndividuals(MongoTemplate mongoTemplate, Collection<Individual> indsToAdd, Collection<GenotypingSample> samplesToAdd, Collection<GenotypingSample> samplesToUpdate) throws InterruptedException {
        final int importChunkSize = 1000;

        // Sample update
        List<Thread> callsetImportThreads = new ArrayList<>();
        if (!samplesToUpdate.isEmpty()) {
        	List<GenotypingSample> samplesToSave = new ArrayList<>(samplesToUpdate);
            for (int j=0; j<Math.ceil((float) samplesToSave.size() / importChunkSize); j++) {
            	final Collection<GenotypingSample> subList = samplesToSave.subList(j * importChunkSize, Math.min(samplesToSave.size(), (j + 1) * importChunkSize));
		        Thread callsetImportThread = new Thread() {
		            public void run() {
		                for (GenotypingSample sample : subList)
		                    mongoTemplate.save(sample);
		            }
		        };
		        callsetImportThread.start();
		        callsetImportThreads.add(callsetImportThread);
            }
        }

        // Sample import
        List<GenotypingSample> samplesToImport = new ArrayList<>(samplesToAdd);
        for (int j=0; j<Math.ceil((float) samplesToAdd.size() / importChunkSize); j++)
            mongoTemplate.insert(samplesToImport.subList(j * importChunkSize, Math.min(samplesToImport.size(), (j + 1) * importChunkSize)), GenotypingSample.class);
        
        // Individual import
        List<Individual> individualsToImport = new ArrayList<>(indsToAdd);
        for (int j=0; j<Math.ceil((float) individualsToImport.size() / importChunkSize); j++)
            mongoTemplate.insert(individualsToImport.subList(j * importChunkSize, Math.min(individualsToImport.size(), (j + 1) * importChunkSize)), Individual.class);

        for (Thread t : callsetImportThreads)
        	t.join();
    }

    public void updateExistingVrdAlleles(MongoTemplate mongoTemplate, int initialAlleleCount, VariantData variant) {
	    if (variant.getKnownAlleles().size() > initialAlleleCount) {
	    	new Thread() {
	    		public void run() {
			    	UpdateResult existingVrdAlleleUpdates = mongoTemplate.updateMulti(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID).is(variant.getId())), new Update().set(VariantData.FIELDNAME_KNOWN_ALLELES, variant.getKnownAlleles()), VariantRunData.class);
			    	if (existingVrdAlleleUpdates.getModifiedCount() > 0)
						LOG.debug("Updated " + existingVrdAlleleUpdates.getModifiedCount() + " existing VRD entries for variant " + variant.getId() + " to reflect new known alleles");
	    		}
	    	}.start();
	    }
    }
}
