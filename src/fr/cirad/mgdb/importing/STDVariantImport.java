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
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;

import fr.cirad.mgdb.importing.base.RefactoredImport;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ExternalSort;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;

public class STDVariantImport extends RefactoredImport {
	
	private static final Logger LOG = Logger.getLogger(STDVariantImport.class);
	
	protected String m_processID;
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
        STDVariantImport instance = new STDVariantImport();
        instance.setMaxExpectedAlleleCount(2);
        instance.importToMongo(args[0], args[1], args[2], args[3], null, true, args[4], args[5], mode);
    }
	
	public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, HashMap<String, String> sampleToIndividualMap, boolean fCheckConsistencyBetweenSynonyms, String mainFilePath, String assemblyName, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		ProgressIndicator progress = ProgressIndicator.get(m_processID);
		if (progress == null)
		{
			progress = new ProgressIndicator(m_processID, new String[] {"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
			ProgressIndicator.registerProgressIndicator(progress);
		}
		
		GenericXmlApplicationContext ctx = null;
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

		File genotypeFile = new File(mainFilePath);
		List<File> sortedTempFiles = null;
        File sortedFile = File.createTempFile("sortedImportFile_-" + genotypeFile.getName() + "-", ".std");
		File variantOrientedFile = File.createTempFile("variantOrientedImportFile_-" + genotypeFile.getName() + "-", ".tsv");

		Integer createdProject = null;
        
		try
		{
			progress.addStep("Scanning existing marker IDs");
			progress.moveToNextStep();
			Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
			HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

            mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));	// disable profiling
			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() > 0 && project.getPloidyLevel() != m_ploidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + m_ploidy + ") data!");
            
			m_fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);
			
			MongoTemplateManager.lockProjectForWriting(sModule, sProject);
			cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

            // create project if necessary
            if (project == null || importMode > 0) {   // create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
//				project.setOrigin(1 /* SNP chip */);
				project.setTechnology(sTechnology);
                if (importMode != 1)
                	createdProject = project.getId();
            }
            project.setPloidyLevel(m_ploidy);
			LOG.info("variant-oriented file will be " + variantOrientedFile.getAbsolutePath());
			
			// sort genotyping data file by marker name 
			BufferedReader in = new BufferedReader(new FileReader(genotypeFile));			
			final Integer finalMarkerFieldIndex = 2, finalSampleFieldIndex = 1;
			Comparator<String> comparator = new Comparator<String>() {						
				@Override
				public int compare(String o1, String o2) {	/* we want data to be sorted, first by locus, then by sample */
					String[] splitted1 = (o1.split(" ")), splitted2 = (o2.split(" "));
					return (splitted1[finalMarkerFieldIndex] + "_" + splitted1[finalSampleFieldIndex]).compareTo(splitted2[finalMarkerFieldIndex] + "_" + splitted2[finalSampleFieldIndex]);
				}
			};
			
			try
			{
				progress.addStep("Creating temp files to sort in batch");
				progress.moveToNextStep();			
				sortedTempFiles = ExternalSort.sortInBatch(in, genotypeFile.length(), comparator, ExternalSort.DEFAULTMAXTEMPFILES, Charset.defaultCharset(), sortedFile.getParentFile(), false, 0, true, progress);
	            if (progress.getError() != null || progress.isAborted())
	                return createdProject;

				long afterSortInBatch = System.currentTimeMillis();
				LOG.info("sortInBatch took " + (afterSortInBatch - before)/1000 + "s");
				
				progress.addStep("Merging temp files");
				progress.moveToNextStep();
				ExternalSort.mergeSortedFiles(sortedTempFiles, sortedFile, comparator, Charset.defaultCharset(), false, false, true, progress, genotypeFile.length());
	            if (progress.getError() != null || progress.isAborted())
	            	return createdProject;

				LOG.info("mergeSortedFiles took " + (System.currentTimeMillis() - afterSortInBatch)/1000 + "s");
			}
	        catch (java.io.IOException ioe)
	        {
	        	LOG.error("Error occured sorting import file", ioe);
	        	return createdProject;
	        }
			
			
			// group data into one variant per line AND build alphabetically-sorted individual-to-population Map
			progress.addStep("Grouping genotyping data lines into one per variant");
			progress.moveToNextStep();
			progress.setPercentageEnabled(false);
			FileWriter fw = new FileWriter(variantOrientedFile);
			Scanner scanner = new Scanner(sortedFile);
			String sInputLine;
			long lineCount = 0;
			String sPreviousVariant = null, sVariantName = null;
			LinkedHashMap<String, String> providedVariantPositions = new LinkedHashMap<>();
			LinkedHashMap<String, String> orderedIndividualToPopulationMap = new LinkedHashMap<>();
            HashMap<String, Integer> individualPositions = new HashMap<>();
			LinkedList<String> genotypeLine = new LinkedList<>();
			while (scanner.hasNextLine()) {
				sInputLine = scanner.nextLine();
				if (sInputLine.length() > 0) {
					String[] splitInputLine = sInputLine.trim().split(" ");
					String sIndividual = splitInputLine[1];
					Integer indPos = individualPositions.get(sIndividual);
					if (indPos == null) {	// it's the first time we encounter this individual
						indPos = individualPositions.size();
						individualPositions.put(sIndividual, indPos);
						genotypeLine.add(null);
						orderedIndividualToPopulationMap.put(sIndividual, splitInputLine[0]);
					}
					sVariantName = splitInputLine[2];
					if (!sVariantName.equals(sPreviousVariant))
					{
						providedVariantPositions.put(sVariantName, null);	// we have no positions provided in this format but we still want to fill in this Map because its size is being used for progress monitoring when important into DB
						if (sPreviousVariant != null) {
							fw.write(sPreviousVariant);
							for (String gt : genotypeLine)
								fw.write(gt != null ? gt : ("\t"));
							genotypeLine = new LinkedList<>();
							for (int i=0; i<individualPositions.size(); i++)
								genotypeLine.add(null);
							fw.write("\n");
						}
						sPreviousVariant = sVariantName;
					}
					StringBuilder gtBuilder = new StringBuilder();
					for (int i=3; i<splitInputLine.length; i++)
						gtBuilder.append(i == 3 ? "\t" : "/").append(splitInputLine[i]);
					genotypeLine.set(indPos, gtBuilder.toString());
				}
				if (++lineCount%100000 == 0)
					progress.setCurrentStepProgress(lineCount);
			}
			if (sVariantName != null) {
				fw.write(sVariantName);
				for (String gt : genotypeLine)
					fw.write(gt != null ? gt : ("\t"));
			}
			fw.close();
			scanner.close();
			sortedFile.delete();
			sortedFile = variantOrientedFile;
			

            // Create the necessary samples
            createSamples(mongoTemplate, project.getId(), sRun, sampleToIndividualMap, orderedIndividualToPopulationMap, progress);
            if (progress.getError() != null || progress.isAborted())
                return createdProject;
            
            
            // Consistency checking (optional)
            HashMap<String, ArrayList<String>> inconsistencies = null;
            HashSet<Integer> indexesOfLinesThatMustBeSkipped = new HashSet<>();
            if (fCheckConsistencyBetweenSynonyms) {
                progress.addStep("Checking genotype consistency between synonyms");
                progress.moveToNextStep();
            	checkSynonymGenotypeConsistency(sortedFile, existingVariantIDs, orderedIndividualToPopulationMap.keySet(), genotypeFile.getParentFile() + File.separator + sModule + "_" + sProject + "_" + sRun, indexesOfLinesThatMustBeSkipped);
            }
            if (progress.getError() != null || progress.isAborted())
                return createdProject;

            
            // Variant-oriented file import
            int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nConcurrentThreads + " threads");

            long count = importTempFileContents(progress, nConcurrentThreads, mongoTemplate, assembly == null ? null : assembly.getId(), sortedFile, providedVariantPositions, existingVariantIDs, project, sRun, inconsistencies, orderedIndividualToPopulationMap, new HashMap<>(), indexesOfLinesThatMustBeSkipped, false);

            if (progress.getError() != null)
                throw new Exception(progress.getError());

            if (!progress.isAborted())
            	LOG.info("GSGTVariantImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
            return createdProject;
		}
        catch (Exception e) {
        	LOG.error("Error", e);
        	progress.setError(e.getMessage());
        	return createdProject;
        }
		finally {
        	// let's cleanup
        	if (sortedFile.exists())
        		sortedFile.delete();
        	if (sortedTempFiles != null)
            	for (File f : sortedTempFiles)
            		if (f.exists())
            			f.delete();
			
			MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
            if (progress.getError() == null && !progress.isAborted()) {
                progress.addStep("Preparing database for searches");
                progress.moveToNextStep();
                MgdbDao.prepareDatabaseForSearches(sModule);
            }

			if (ctx != null)
				ctx.close();
		}
	}
}
