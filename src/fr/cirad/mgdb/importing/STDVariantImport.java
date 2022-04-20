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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

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
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
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
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class STDVariantImport extends AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(STDVariantImport.class);
	
	private int m_ploidy = 2;
	private String m_processID;
	private boolean fImportUnknownVariants = false;
	private boolean m_fTryAndMatchRandomObjectIDs = false;
	
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
		if (args.length < 5)
			throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, GENOTYPE file! An optional 6th parameter supports values '1' (empty project data before importing) and '2' (empty entire database before importing, including marker list).");

		File mainFile = new File(args[4]);
		if (!mainFile.exists() || mainFile.length() == 0)
			throw new Exception("File " + args[4] + " is missing or empty!");
		
		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[5]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new STDVariantImport().importToMongo(args[0], args[1], args[2], args[3], args[4], mode);
	}
	
	public void importToMongo(String sModule, String sProject, String sRun, String sTechnology, String mainFilePath, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		ProgressIndicator progress = ProgressIndicator.get(m_processID);
		if (progress == null)
		{
			progress = new ProgressIndicator(m_processID, new String[] {"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
			ProgressIndicator.registerProgressIndicator(progress);
		}
		
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(sModule);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
			}
			
			mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));	// disable profiling
			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() > 0 && project.getPloidyLevel() != m_ploidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + m_ploidy + ") data!");
            
			fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);
			
			MongoTemplateManager.lockProjectForWriting(sModule, sProject);
			
			cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);
			
			progress.addStep("Reading marker IDs");
			progress.moveToNextStep();
			
			File genotypeFile = new File(mainFilePath);
	
            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, m_fTryAndMatchRandomObjectIDs);
						
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
			File sortedFile = new File("sortedImportFile_" + genotypeFile.getName());
			sortedFile.deleteOnExit();
			LOG.info("Sorted file will be " + sortedFile.getAbsolutePath());
			
			List<File> sortTempFiles = null;
			try
			{
				progress.addStep("Creating temp files to sort in batch");
				progress.moveToNextStep();			
				sortTempFiles = ExternalSort.sortInBatch(in, genotypeFile.length(), comparator, ExternalSort.DEFAULTMAXTEMPFILES, Charset.defaultCharset(), sortedFile.getParentFile(), false, 0, true, progress);
				long afterSortInBatch = System.currentTimeMillis();
				LOG.info("sortInBatch took " + (afterSortInBatch - before)/1000 + "s");
				
				progress.addStep("Merging temp files");
				progress.moveToNextStep();
				ExternalSort.mergeSortedFiles(sortTempFiles, sortedFile, comparator, Charset.defaultCharset(), false, false, true, progress, genotypeFile.length());
				LOG.info("mergeSortedFiles took " + (System.currentTimeMillis() - afterSortInBatch)/1000 + "s");
			}
	        catch (java.io.IOException ioe)
	        {
	        	// it failed: let's cleanup
	        	if (sortTempFiles != null)
	            	for (File f : sortTempFiles)
	            		f.delete();
	        	if (sortedFile.exists())
	        		sortedFile.delete();
	        	LOG.error("Error occured sorting import file", ioe);
	        	return;
	        }

			// create project if necessary
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
			TreeSet<String> affectedSequences = new TreeSet<String>();	// will contain all sequences containing variants for which we are going to add genotypes 
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
							else if (saveWithOptimisticLock(mongoTemplate, project, sRun, mgdbVariantId != null ? mgdbVariantId : sPreviousVariant, individualPopulations, inconsistencies, linesForVariant, 3, previouslyCreatedSamples, affectedSequences))
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
			while (sLine != null);		

			String mgdbVariantId = existingVariantIDs.get(sVariantName.toUpperCase());	// when saving the last variant there is not difference between sVariantName and sPreviousVariant
			if (mgdbVariantId == null && !fImportUnknownVariants)
				LOG.warn("Skipping unknown variant: " + mgdbVariantId);
			else if (mgdbVariantId != null && mgdbVariantId.toString().startsWith("*"))
				LOG.warn("Skipping deprecated variant data: " + sPreviousVariant);
			else if (saveWithOptimisticLock(mongoTemplate, project, sRun, mgdbVariantId != null ? mgdbVariantId : sPreviousVariant, individualPopulations, inconsistencies, linesForVariant, 3, previouslyCreatedSamples, affectedSequences))
				nVariantSaveCount++;
			else
				unsavedVariants.add(sVariantName);
	
			in.close();
			sortedFile.delete();
							
			// save project data
            if (!project.getVariantTypes().contains(Type.SNP.toString())) {
                project.getVariantTypes().add(Type.SNP.toString());
            }
            project.getSequences().addAll(affectedSequences);
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            if (project.getPloidyLevel() == 0)
            	project.setPloidyLevel(m_ploidy);
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
			if (ctx != null)
				ctx.close();
			
			MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
		}
	}
	
	private boolean saveWithOptimisticLock(MongoTemplate mongoTemplate, GenotypingProject project, String runName, String mgdbVariantId, HashMap<String, String> individualPopulations, HashMap<String, ArrayList<String>> inconsistencies, ArrayList<String> linesForVariant, int nNumberOfRetries, Map<String, GenotypingSample> usedSamples, TreeSet<String> affectedSequences) throws Exception
	{
		if (linesForVariant.size() == 0)
			return false;
		
		for (int j=0; j<Math.max(1, nNumberOfRetries); j++)
		{			
			Query query = new Query(Criteria.where("_id").is(mgdbVariantId));
			query.fields().include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_KNOWN_ALLELES).include(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId()).include(VariantData.FIELDNAME_VERSION);
			
			try {
    			VariantData variant = mongoTemplate.findOne(query, VariantData.class);
    			Update update = variant == null ? null : new Update();
    			if (update == null)
    			{	// it's the first time we deal with this variant
    				variant = new VariantData((ObjectId.isValid(mgdbVariantId) ? "_" : "") + mgdbVariantId);
    				variant.setType(Type.SNP.toString());
    			}
    			else
    			{
    				ReferencePosition rp = variant.getReferencePosition();
    				if (rp != null)
    					affectedSequences.add(rp.getSequence());
    			}
    			
    			String sVariantName = linesForVariant.get(0).trim().split(" ")[2];
    //			if (!mgdbVariantId.equals(sVariantName))
    //				variant.setSynonyms(markerSynonymMap.get(mgdbVariantId));	// provided id was a synonym
    			
    			VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, mgdbVariantId));
    			
    			ArrayList<String> inconsistentIndividuals = inconsistencies.get(mgdbVariantId);
    			for (String individualLine : linesForVariant)
    			{				
    				String[] cells = individualLine.trim().split(" ");
    				String sIndividual = cells[1];
    						
    				if (!usedSamples.containsKey(sIndividual))	// we don't want to persist each sample several times
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
    	                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
    	                usedSamples.put(sIndividual, new GenotypingSample(sampleId, project.getId(), vrd.getRunName(), sIndividual));	// add a sample for this individual to the project
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
    						int indexToUse = cells.length == 3 + m_ploidy ? i : 3;	// support for collapsed homozygous genotypes
    						if (!variant.getKnownAlleles().contains(cells[indexToUse]))
    						{
    							variant.getKnownAlleles().add(cells[indexToUse]);	// it's the first time we encounter this alternate allele for this variant
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
    					continue;	// we don't add missing genotypes
    				
    				SampleGenotype genotype = new SampleGenotype(gtCode);
    				vrd.getSampleGenotypes().put(usedSamples.get(sIndividual).getId(), genotype);
    			}
                project.getAlleleCounts().add(variant.getKnownAlleles().size());	// it's a TreeSet so it will only be added if it's not already present
    			
    			try
    			{
    				if (update == null)
    				{
    					mongoTemplate.save(variant);
    //					System.out.println("saved: " + variant.getId());
    				}
    				else if (!update.getUpdateObject().keySet().isEmpty())
    				{
    //					update.set(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId(), projectData);
    					mongoTemplate.upsert(new Query(Criteria.where("_id").is(mgdbVariantId)).addCriteria(Criteria.where(VariantData.FIELDNAME_VERSION).is(variant.getVersion())), update, VariantData.class);
    //					System.out.println("updated: " + variant.getId());
    				}
    		        vrd.setKnownAlleles(variant.getKnownAlleles());
    		        vrd.setReferencePosition(variant.getReferencePosition());
    		        vrd.setType(Type.SNP.toString());
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
			catch (Exception e) {
			    e.printStackTrace();
			}
		}
		return false;	// all attempts failed
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
}
