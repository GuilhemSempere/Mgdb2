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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BasicDBObject;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ExternalSort;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class STDVariantImport extends AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(STDVariantImport.class);
	
	private int m_ploidy = 2;
	private String m_processID;
	private boolean fImportUnknownVariants = false;
	private boolean m_fTryAndMatchRandomObjectIDs = false;

	private HashMap<String, String> individualToSampleIdMap = null;
	
	public STDVariantImport()
	{
	}
	
	public STDVariantImport(String processID)
	{
		this();
		m_processID = processID;
	}
	
	public boolean triesAndMatchRandomObjectIDs() {
		return m_fTryAndMatchRandomObjectIDs;
	}

	public void tryAndMatchRandomObjectIDs(boolean fTryAndMatchRandomObjectIDs) {
		this.m_fTryAndMatchRandomObjectIDs = fTryAndMatchRandomObjectIDs;
	}
	
    public static void main(String[] args) throws Exception
    {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, GENOTYPE file, assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty entire database before importing, including marker list).");

        File mainFile = new File(args[4]);
        if (!mainFile.exists() || mainFile.length() == 0)
            throw new Exception("File " + args[4] + " is missing or empty!");
        
        int mode = 0;
        try
        {
            mode = Integer.parseInt(args[6]);
        }
        catch (Exception e)
        {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        new STDVariantImport().importToMongo(args[0], args[1], args[2], args[3], args[4], args[5], mode);
    }
	
	public void importToMongo(String sModule, String sProject, String sRun, String sTechnology, String mainFilePath, String assemblyName, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		ProgressIndicator progress = ProgressIndicator.get(m_processID);
		if (progress == null)
		{
			progress = new ProgressIndicator(m_processID, new String[] {"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
			ProgressIndicator.registerProgressIndicator(progress);
		}
		
		GenericXmlApplicationContext ctx = null;
		File genotypeFile = new File(mainFilePath);
		List<File> sortTempFiles = null;
		File sortedFile = new File("sortedImportFile_" + genotypeFile.getName());
		sortedFile.deleteOnExit();	//just to be sure

		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		if (mongoTemplate == null) { // we are probably being invoked offline
			ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

			MongoTemplateManager.initialize(ctx);
			mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(sModule);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
			}
		}

		try
		{
            HashMap<String, String> existingVariantIDs;
            Assembly assembly = mongoTemplate.findOne(new Query(Criteria.where(Assembly.FIELDNAME_NAME).is(assemblyName)), Assembly.class);
            if (assembly == null) {
                if ("".equals(assemblyName) || m_fAllowNewAssembly) {
                    assembly = new Assembly("".equals(assemblyName) ? 0 : AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(Assembly.class)));
                    assembly.setName(assemblyName);
                    mongoTemplate.save(assembly);
                    existingVariantIDs = new HashMap<>();
                }
                else
                    throw new Exception("Assembly \"" + assemblyName + "\" not found in database. Supported assemblies are " + StringUtils.join(mongoTemplate.findDistinct(Assembly.FIELDNAME_NAME, Assembly.class, String.class), ", "));
            }
            else {
                progress.addStep("Reading marker IDs");
                progress.moveToNextStep();
                existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false, assembly.getId());
            }

            mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));	// disable profiling
			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() > 0 && project.getPloidyLevel() != m_ploidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + m_ploidy + ") data!");
            
			fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);
			
			MongoTemplateManager.lockProjectForWriting(sModule, sProject);
			
			cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

			progress.addStep("Checking genotype consistency");
			progress.moveToNextStep();

			HashMap<String, ArrayList<String>> inconsistencies = checkSynonymGenotypeConsistency(existingVariantIDs, genotypeFile, sModule + "_" + sProject + "_" + sRun);
			
			// first sort genotyping data file by marker name (for faster import)
			BufferedReader in = new BufferedReader(new FileReader(genotypeFile));
			
			final Integer finalMarkerFieldIndex = 2;
			Comparator<String> comparator = new Comparator<String>() {						
				@Override
				public int compare(String o1, String o2) {	/* we want data to be sorted, first by locus, then by sample */
					String[] splitted1 = (o1.split(" ")), splitted2 = (o2.split(" "));
					return (splitted1[finalMarkerFieldIndex]/* + "_" + splitted1[finalSampleFieldIndex]*/).compareTo(splitted2[finalMarkerFieldIndex]/* + "_" + splitted2[finalSampleFieldIndex]*/);
				}
			};
			LOG.info("Sorted file will be " + sortedFile.getAbsolutePath());
			
			try
			{
				progress.addStep("Creating temp files to sort in batch");
				progress.moveToNextStep();			
				sortTempFiles = ExternalSort.sortInBatch(in, genotypeFile.length(), comparator, ExternalSort.DEFAULTMAXTEMPFILES, Charset.defaultCharset(), sortedFile.getParentFile(), false, 0, true, progress);
	            if (progress.getError() != null || progress.isAborted())
	                return;

				long afterSortInBatch = System.currentTimeMillis();
				LOG.info("sortInBatch took " + (afterSortInBatch - before)/1000 + "s");
				
				progress.addStep("Merging temp files");
				progress.moveToNextStep();
				ExternalSort.mergeSortedFiles(sortTempFiles, sortedFile, comparator, Charset.defaultCharset(), false, false, true, progress, genotypeFile.length());
	            if (progress.getError() != null || progress.isAborted())
	                return;

				LOG.info("mergeSortedFiles took " + (System.currentTimeMillis() - afterSortInBatch)/1000 + "s");
			}
	        catch (java.io.IOException ioe)
	        {
	        	LOG.error("Error occured sorting import file", ioe);
	        	return;
	        }

			if (project == null)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(1 /* SNP chip */);
				project.setTechnology(sTechnology);
				project.getVariantTypes().add(Type.SNP.toString());
			}

			// import genotyping data
			progress.addStep("Processing genotype lines by thousands");
			progress.moveToNextStep();
			progress.setPercentageEnabled(false);
			HashMap<String, String> individualPopulations = new HashMap<String, String>();				
			in = new BufferedReader(new FileReader(sortedFile));
			String sLine = in.readLine();
			int nVariantSaveCount = 0;
			long lineCount = 0;
			String sPreviousVariant = null, sVariantName = null;
			ArrayList<String> linesForVariant = new ArrayList<String>(), unsavedVariants = new ArrayList<String>();
			TreeMap<String /* individual name */, GenotypingSample> previouslyCreatedSamples = new TreeMap<>();	// will auto-magically remove all duplicates, and sort data, cool eh?
            Map<Integer, TreeSet<String>> affectedSequencesByAssembly = new HashMap<>();    // will contain all sequences containing variants for which we are going to add genotypes
            mongoTemplate.findAll(Assembly.class).stream().forEach(asm -> affectedSequencesByAssembly.put(asm.getId(), null));
			m_providedIdToSampleMap = new TreeMap<>();	// will auto-magically remove all duplicates, and sort data
			do
			{
				if (sLine.length() > 0)
				{
					String[] splittedLine = sLine.trim().split(" ");
					sVariantName = splittedLine[2];
					individualPopulations.put(splittedLine[1], splittedLine[0]);
					if (!sVariantName.equals(sPreviousVariant))
					{
						if (sPreviousVariant != null)
						{	// save variant
							String mgdbVariantId = existingVariantIDs.get(sPreviousVariant.toUpperCase());
							if (mgdbVariantId == null && !fImportUnknownVariants)
								LOG.warn("Skipping unknown variant: " + mgdbVariantId);
							else if (mgdbVariantId != null && mgdbVariantId.toString().startsWith("*"))
								LOG.warn("Skipping deprecated variant data: " + sPreviousVariant);
							else if (saveWithOptimisticLock(mongoTemplate, project, sRun, mgdbVariantId != null ? mgdbVariantId : sPreviousVariant, individualPopulations, inconsistencies, linesForVariant, 3, affectedSequencesByAssembly))
								nVariantSaveCount++;
							else
								unsavedVariants.add(sVariantName);
						}
						linesForVariant = new ArrayList<String>();
						sPreviousVariant = sVariantName;
					}
					linesForVariant.add(sLine);		
				}
				sLine = in.readLine();
				progress.setCurrentStepProgress((int) lineCount/1000);
				if (++lineCount % 100000 == 0)
				{
					String info = lineCount + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
					LOG.info(info);
				}
			}
			while (sLine != null && progress.getError() == null && !progress.isAborted());
			
            if (progress.getError() != null || progress.isAborted())
                return;

			String mgdbVariantId = existingVariantIDs.get(sVariantName.toUpperCase());	// when saving the last variant there is not difference between sVariantName and sPreviousVariant
			if (mgdbVariantId == null && !fImportUnknownVariants)
				LOG.warn("Skipping unknown variant: " + mgdbVariantId);
			else if (mgdbVariantId != null && mgdbVariantId.toString().startsWith("*"))
				LOG.warn("Skipping deprecated variant data: " + sPreviousVariant);
			else if (saveWithOptimisticLock(mongoTemplate, project, sRun, mgdbVariantId != null ? mgdbVariantId : sPreviousVariant, individualPopulations, inconsistencies, linesForVariant, 3, affectedSequencesByAssembly))
				nVariantSaveCount++;
			else
				unsavedVariants.add(sVariantName);
	
			in.close();
			
			// we only know for sure the entire list of individuals / samples at the end of the process (one genotype per line means we may find out some new individuals along with the very last variant)
            mongoTemplate.insert(m_providedIdToSampleMap.values(), GenotypingSample.class);
			m_fSamplesPersisted = true;
							
			// save project data
            if (!project.getVariantTypes().contains(Type.SNP.toString()))
                project.getVariantTypes().add(Type.SNP.toString());
            for (Integer anAssemblyId : affectedSequencesByAssembly.keySet()) {
            	TreeSet<String> affectedSequencesForssembly = affectedSequencesByAssembly.get(anAssemblyId);
            	if (affectedSequencesForssembly != null)
            		project.getContigs(anAssemblyId).addAll(affectedSequencesForssembly);
            }
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            if (project.getPloidyLevel() == 0)
            	project.setPloidyLevel(m_ploidy);
			mongoTemplate.save(project);
	
	    	LOG.info("Import took " + (System.currentTimeMillis() - before)/1000 + "s for " + lineCount + " CSV lines (" + nVariantSaveCount + " variants were saved)");
	    	if (unsavedVariants.size() > 0)
	    	   	LOG.warn("The following variants could not be saved because of concurrent writing: " + StringUtils.join(unsavedVariants, ", "));
		}
		finally
		{
        	// let's cleanup
        	if (sortedFile.exists())
        		sortedFile.delete();
        	if (sortTempFiles != null)
            	for (File f : sortTempFiles)
            		if (f.exists())
            			f.delete();
        	
			if (ctx != null)
				ctx.close();
			
			MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
            if (progress.getError() == null && !progress.isAborted()) {
                progress.addStep("Preparing database for searches");
                progress.moveToNextStep();
                MgdbDao.prepareDatabaseForSearches(sModule);
            }
		}
	}

	private boolean saveWithOptimisticLock(MongoTemplate mongoTemplate, GenotypingProject project, String runName, String mgdbVariantId, HashMap<String, String> individualPopulations, HashMap<String, ArrayList<String>> inconsistencies, ArrayList<String> linesForVariant, int nNumberOfRetries, Map<Integer, TreeSet<String>> affectedSequencesByAssembly) throws Exception
	{
	    if (linesForVariant.size() == 0)
	        return false;
	    
	    for (int j=0; j<Math.max(1, nNumberOfRetries); j++)
	    {           
	        Query query = new Query(Criteria.where("_id").is(mgdbVariantId));
	        query.fields().include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_KNOWN_ALLELES).include(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId()).include(VariantData.FIELDNAME_VERSION);
	        
	        VariantData variant = mongoTemplate.findOne(query, VariantData.class);
	        Update update = variant == null ? null : new Update();
	        if (update == null)
	        {   // it's the first time we deal with this variant
	            variant = new VariantData((ObjectId.isValid(mgdbVariantId) ? "_" : "") + mgdbVariantId);
	            variant.setType(Type.SNP.toString());
	        }
	        else
	        	for (Integer anAssemblyId : affectedSequencesByAssembly.keySet()) {
	                ReferencePosition rp = variant.getPositions().get(anAssemblyId);
	                if (rp != null) {
	                    TreeSet<String> affectedSequencesForAssembly = affectedSequencesByAssembly.get(anAssemblyId);
	                    if (affectedSequencesForAssembly == null) {
	                        affectedSequencesForAssembly = new TreeSet<>();
	                        affectedSequencesByAssembly.put(anAssemblyId, affectedSequencesForAssembly);
	                    }
	                    affectedSequencesForAssembly.add(rp.getSequence());
	                }
	            }
	        
	        
	        String sVariantName = linesForVariant.get(0).trim().split(" ")[2];
	//      if (!mgdbVariantId.equals(sVariantName))
	//          variant.setSynonyms(markerSynonymMap.get(mgdbVariantId));   // provided id was a synonym
	        
	        VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, mgdbVariantId));
	        
	        ArrayList<String> inconsistentIndividuals = inconsistencies.get(mgdbVariantId);
	        for (String individualLine : linesForVariant)
	        {               
	            String[] cells = individualLine.trim().split(" ");
	            String sIndividual = cells[1];
	                    
	            GenotypingSample sample = m_providedIdToSampleMap.get(sIndividual);
	            if (sample == null) // we don't want to persist each sample several times
	            {
	                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
	                String sPop = individualPopulations.get(sIndividual);
	                boolean fAlreadyExists = ind != null, fNeedToSave = true;
	                if (!fAlreadyExists) {
	                    ind = new Individual(sIndividual);
	                    ind.setPopulation(sPop);
	                }
	                else if (sPop.equals(ind.getPopulation()))
	                    fNeedToSave = false;
	                else {
	                    if (ind.getPopulation() != null)
	                        LOG.warn("Changing individual " + sIndividual + "'s population from " + ind.getPopulation() + " to " + sPop);
	                    ind.setPopulation(sPop);
	                }
	                if (fNeedToSave)
	                    mongoTemplate.save(ind);
	                sample = new GenotypingSample(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class)), project.getId(), vrd.getRunName(), sIndividual, individualToSampleIdMap == null ? null : individualToSampleIdMap.get(sIndividual));
	                m_providedIdToSampleMap.put(sIndividual, sample);   // add a sample for this individual to the project
	            }
	
	            String gtCode = null;
	            boolean fInconsistentData = inconsistentIndividuals != null && inconsistentIndividuals.contains(sIndividual);
	            if (fInconsistentData)
	                LOG.warn("Not adding inconsistent data: " + sVariantName + " / " + sIndividual);
	            else if (cells.length > 3)
	            {
	                ArrayList<Integer> alleleIndexList = new ArrayList<Integer>();  
	                boolean fAddedSomeAlleles = false;
	                for (int i=3; i<3 + m_ploidy; i++)
	                {
	                    int indexToUse = cells.length == 3 + m_ploidy ? i : 3;  // support for collapsed homozygous genotypes
	                    if (!variant.getKnownAlleles().contains(cells[indexToUse]))
	                    {
	                        variant.getKnownAlleles().add(cells[indexToUse]);   // it's the first time we encounter this alternate allele for this variant
	                        fAddedSomeAlleles = true;
	                    }
	                    
	                    alleleIndexList.add(variant.getKnownAlleles().indexOf(cells[indexToUse]));
	                }
	                
	                if (fAddedSomeAlleles && update != null)
	                    update.set(VariantData.FIELDNAME_KNOWN_ALLELES, variant.getKnownAlleles());
	
	                Collections.sort(alleleIndexList);
	                gtCode = StringUtils.join(alleleIndexList, "/");
	            }
	
	            if (gtCode == null)
	                continue;   // we don't add missing genotypes
	            
	            SampleGenotype genotype = new SampleGenotype(gtCode);
	            vrd.getSampleGenotypes().put(sample.getId(), genotype);
	        }
	        project.getAlleleCounts().add(variant.getKnownAlleles().size());    // it's a TreeSet so it will only be added if it's not already present
	        
	        try
	        {
	            if (update == null)
	            {
	                mongoTemplate.save(variant);
	//              System.out.println("saved: " + variant.getId());
	            }
	            else if (!update.getUpdateObject().keySet().isEmpty())
	            {
	//              update.set(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId(), projectData);
	                mongoTemplate.upsert(new Query(Criteria.where("_id").is(mgdbVariantId)).addCriteria(Criteria.where(VariantData.FIELDNAME_VERSION).is(variant.getVersion())), update, VariantData.class);
	//              System.out.println("updated: " + variant.getId());
	            }
	
	            vrd.setKnownAlleles(variant.getKnownAlleles());
	            vrd.setPositions(variant.getPositions());
	            vrd.setType(Type.SNP.toString());
	            vrd.setSynonyms(variant.getSynonyms());
	            mongoTemplate.save(vrd);
	
	            if (j > 0)
	                LOG.info("It took " + j + " retries to save variant " + variant.getId());
	            return true;
	        }
	        catch (OptimisticLockingFailureException olfe)
	        {
	//          LOG.info("failed: " + variant.getId());
	        }
	    }
	    return false;   // all attempts failed
	}
	
	private static HashMap<String, ArrayList<String>> checkSynonymGenotypeConsistency(HashMap<String, String> markerIDs, File stdFile, String outputFilePrefix) throws IOException
	{
		long before = System.currentTimeMillis();
		BufferedReader in = new BufferedReader(new FileReader(stdFile));
		String sLine;
		final String separator = " ";
		long lineCount = 0;
		String sPreviousSample = null, sSampleName = null;
		HashMap<String /*mgdb variant id*/, HashMap<String /*genotype*/, String /*synonyms*/>> genotypesByVariant = new HashMap<>();

		LOG.info("Checking genotype consistency between synonyms...");
		
		FileOutputStream inconsistencyFOS = new FileOutputStream(new File(stdFile.getParentFile() + File.separator + outputFilePrefix + "-INCONSISTENCIES.txt"));
		HashMap<String /*mgdb variant id*/, ArrayList<String /*individual*/>> result = new HashMap<>();
		
		while ((sLine = in.readLine()) != null)	
		{
			if (sLine.length() > 0)
			{
				String[] splittedLine = sLine.trim().split(separator);
				String mgdbId = markerIDs.get(splittedLine[2].toUpperCase());
				if (mgdbId == null)
					mgdbId = splittedLine[2];
				else if (mgdbId.toString().startsWith("*"))
					continue;	// this is a deprecated variant

				sSampleName = splittedLine[1];
				if (!sSampleName.equals(sPreviousSample))
				{				
					genotypesByVariant = new HashMap<>();
					sPreviousSample = sSampleName;
				}
				
				HashMap<String, String> synonymsByGenotype = genotypesByVariant.get(mgdbId);
				if (synonymsByGenotype == null)
				{
					synonymsByGenotype = new HashMap<String, String>();
					genotypesByVariant.put(mgdbId, synonymsByGenotype);
				}

				String genotype = splittedLine.length < 4 ? "" : (splittedLine[3] + "," + splittedLine[splittedLine.length > 4 ? 4 : 3]);
				String synonymsWithGenotype = synonymsByGenotype.get(genotype);
				synonymsByGenotype.put(genotype, synonymsWithGenotype == null ? splittedLine[2] : (synonymsWithGenotype + ";" + splittedLine[2]));
				if (synonymsByGenotype.size() > 1)
				{
					ArrayList<String> individualsWithInconsistentGTs = result.get(mgdbId);
					if (individualsWithInconsistentGTs == null)
					{
						individualsWithInconsistentGTs = new ArrayList<String>();
						result.put(mgdbId, individualsWithInconsistentGTs);
					}
					individualsWithInconsistentGTs.add(sSampleName);

					inconsistencyFOS.write(sSampleName.getBytes());
					for (String gt : synonymsByGenotype.keySet())
						inconsistencyFOS.write(("\t" + synonymsByGenotype.get(gt) + "=" + gt).getBytes());
					inconsistencyFOS.write("\r\n".getBytes());
				}
			}
			if (++lineCount%1000000 == 0)
				LOG.debug(lineCount + " lines processed (" + (System.currentTimeMillis() - before)/1000 + " sec) ");
		}
		in.close();
		inconsistencyFOS.close();
		
		LOG.info("Inconsistency and missing data file was saved to the following location: " + stdFile.getParentFile().getAbsolutePath());

		return result;
	}

	public void setPloidy(int ploidy) {
		m_ploidy = ploidy;
	}

	public void setIndividualToSampleIdMap(HashMap<String, String> individualToSampleIdMap) {
		this.individualToSampleIdMap  = individualToSampleIdMap;
	}
}
