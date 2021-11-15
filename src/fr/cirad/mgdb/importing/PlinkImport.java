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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.sun.org.apache.xpath.internal.functions.WrongNumberArgsException;

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
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.genotypes.PlinkEigenstratTool;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * The Class PlinkImport.
 */
public class PlinkImport extends AbstractGenotypeImport {

    /** The Constant LOG. */
    private static final Logger LOG = Logger.getLogger(VariantData.class);
    
    /** The m_process id. */
    private String m_processID;
    
    private boolean fImportUnknownVariants = false;
    
    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport()
    {
    }

    public PlinkImport(String processID)
    {
        m_processID = processID;
    }

    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(boolean fCloseContextOpenAfterImport) {
        this();
        m_fCloseContextAfterImport = fCloseContextOpenAfterImport;
    }
    
    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this();
        this.m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(String processID, boolean fCloseContextOpenAfterImport) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextOpenAfterImport;
    }

    
    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(String processID, boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception
    {
        if (args.length < 7)
            throw new Exception("You must pass 7 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, MAP file, and PED file, assembly name! An optional 8th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

        File mapFile = new File(args[4]);
        if (!mapFile.exists() || mapFile.length() == 0)
            throw new Exception("File " + args[4] + " is missing or empty!");
        
        File pedFile = new File(args[5]);
        if (!pedFile.exists() || pedFile.length() == 0)
            throw new Exception("File " + args[5] + " is missing or empty!");

        int mode = 0;
        try
        {
            mode = Integer.parseInt(args[7]);
        }
        catch (Exception e)
        {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        new PlinkImport().importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), new File(args[5]), args[6], mode);
    }

    /**
     * Import to mongo.
     *
     * @param sModule the module
     * @param sProject the project
     * @param sRun the run
     * @param sTechnology the technology
     * @param mapFileURL the map file URL
     * @param pedFile the ped file
     * @param assemblyName the assembly name
     * @param importMode the import mode
     * @return a project ID if it was created by this method, otherwise null
     * @throws Exception the exception
     */
    public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL mapFileURL, File pedFile, String assemblyName, int importMode) throws Exception
    {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"}); // better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
        progress.setPercentageEnabled(false);       

        LinkedHashSet<Integer> redundantVariantIndexes = new LinkedHashSet<>();
        
        GenericXmlApplicationContext ctx = null;
        try
        {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null)
            {   // we are probably being invoked offline
                try
                {
                    ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
                }
                catch (BeanDefinitionStoreException fnfe)
                {
                    LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
                    ctx = new GenericXmlApplicationContext("applicationContext.xml");
                }

                MongoTemplateManager.initialize(ctx);
                mongoTemplate = MongoTemplateManager.get(sModule);
                if (mongoTemplate == null)
                    throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
            }

            fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);          

            if (m_processID == null)
                m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

            mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));  // disable profiling
            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() != 2)
                throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + 2 + ") data!");
            
            cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

            Integer createdProject = null;
            // create project if necessary
            if (project == null || importMode == 2)
            {   // create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
                project.setOrigin(2 /* Sequencing */);
                project.setTechnology(sTechnology);
                createdProject = project.getId();
            }
            project.setPloidyLevel(2);

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
            else
                existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false, assembly.getId());
            
            String info = "Loading variant list from MAP file";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            LinkedHashMap<String, String> variantsAndPositions = PlinkEigenstratTool.getVariantsAndPositionsFromPlinkMapFile(mapFileURL, redundantVariantIndexes, "\t");
            String[] variants = variantsAndPositions.keySet().toArray(new String[variantsAndPositions.size()]);
            
            info = "Checking genotype consistency";
//          LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            
            if (!project.getVariantTypes().contains(Type.SNP.toString()))
                project.getVariantTypes().add(Type.SNP.toString());

            // rotate matrix using temporary files
            info = "Reading and reorganizing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();          
            HashMap<String, String> userIndividualToPopulationMapToFill = new LinkedHashMap<>();
            File[] tempFiles = rotatePlinkPedFile(variants, pedFile, userIndividualToPopulationMapToFill, progress);
            
            progress.addStep("Checking genotype consistency between synonyms");
            progress.moveToNextStep();

            HashMap<String, ArrayList<String>> inconsistencies = checkSynonymGenotypeConsistency(tempFiles, existingVariantIDs, userIndividualToPopulationMapToFill.keySet(), pedFile.getParentFile() + File.separator + sModule + "_" + sProject + "_" + sRun);                        
            if (progress.getError() != null)
                return 0;
            
            
            int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nNConcurrentThreads + " threads");
            long count = importTempFileContents(progress, nNConcurrentThreads, mongoTemplate, assembly.getId(), tempFiles, variantsAndPositions, existingVariantIDs, project, sRun, inconsistencies, userIndividualToPopulationMapToFill);

            progress.addStep("Preparing database for searches");
            progress.moveToNextStep();
            MgdbDao.prepareDatabaseForSearches(mongoTemplate);

            LOG.info("PlinkImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
            progress.markAsComplete();
            return createdProject;
        }
        finally
        {
            if (m_fCloseContextAfterImport && ctx != null)
                ctx.close();
        }
    }

    public long importTempFileContents(ProgressIndicator progress, int nNConcurrentThreads, MongoTemplate mongoTemplate, Integer nAssemblyId, File[] tempFiles, LinkedHashMap<String, String> variantsAndPositions, HashMap<String, String> existingVariantIDs, GenotypingProject project, String sRun, HashMap<String, ArrayList<String>> inconsistencies, Map<String, String> userIndividualToPopulationMap) throws Exception         
    {
        String[] individuals = userIndividualToPopulationMap.keySet().toArray(new String[userIndividualToPopulationMap.size()]);
        HashSet<VariantData> unsavedVariants = new HashSet<VariantData>();  // HashSet allows no duplicates
        HashSet<VariantRunData> unsavedRuns = new HashSet<VariantRunData>();
        int count = 0;
        
        // loop over each variation and write to DB
        Scanner scanner = null;
        try
        {
            String info = "Importing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            progress.setPercentageEnabled(true);
            
            int nNumberOfVariantsToSaveAtOnce = 1;
            HashMap<String /*individual*/, GenotypingSample> previouslyCreatedSamples = new HashMap<>();
            
            final ArrayList<Thread> threadsToWaitFor = new ArrayList<>();
            int chunkIndex = 0;
            
            for (File tempFile : tempFiles) {
                scanner = new Scanner(tempFile);
                while (scanner.hasNextLine())
                {
                    if (progress.getError() != null || progress.isAborted())
                        return count;

                    String[] splitLine = scanner.nextLine().split(" ");
                    String providedVariantId = splitLine[0];

                    String[] seqAndPos = variantsAndPositions.get(providedVariantId).split("\t");
                    String sequence = seqAndPos[0];
                    Long bpPosition = 0l;
                    try
                    {
                        bpPosition = Long.parseLong(seqAndPos[1]);
                    }
                    catch (NumberFormatException nfe)
                    {
                        LOG.warn("Unable to read position for variant " + providedVariantId + " - " + nfe.getMessage());
                    }
                    if ("0".equals(sequence) || 0 == bpPosition)
                    {
                        sequence = null;
                        bpPosition = null;
                    }
                    String variantId = null;
                    for (String variantDescForPos : getIdentificationStrings(Type.SNP.toString(), sequence, bpPosition, Arrays.asList(new String[] {providedVariantId})))
                    {
                        variantId = existingVariantIDs.get(variantDescForPos);
                        if (variantId != null)
                            break;
                    }

                    if (variantId == null && !fImportUnknownVariants)
                        LOG.warn("Skipping unknown variant: " + providedVariantId);
                    else if (variantId != null && variantId.toString().startsWith("*"))
                    {
                        LOG.warn("Skipping deprecated variant data: " + providedVariantId);
                        continue;
                    }
                    else
                    {
                        VariantData variant = mongoTemplate.findById(variantId == null ? providedVariantId : variantId, VariantData.class);                         
                        if (variant == null)
                            variant = new VariantData((ObjectId.isValid(providedVariantId) ? "_" : "") + providedVariantId);

                        String[][] alleles = new String[2][individuals.length];
                        int nIndividualIndex = 0;
                        while (nIndividualIndex < individuals.length)
                        {
                            String[] genotype = splitLine[nIndividualIndex + 1].split("/");
                            if (inconsistencies != null && !inconsistencies.isEmpty()) {
                                ArrayList<String> inconsistentIndividuals = inconsistencies.get(variant.getId());
                                boolean fInconsistentData = inconsistencies != null && !inconsistencies.isEmpty() && inconsistentIndividuals != null && inconsistentIndividuals.contains(individuals[nIndividualIndex]);
                                if (fInconsistentData)
                                    LOG.warn("Not adding inconsistent data: " + providedVariantId + " / " + individuals[nIndividualIndex]);

                                alleles[0][nIndividualIndex] = fInconsistentData ? "0" : genotype[0];
                                alleles[1][nIndividualIndex++] = fInconsistentData ? "0" : genotype[1];
                            }
                            else {
                                alleles[0][nIndividualIndex] = genotype[0];
                                alleles[1][nIndividualIndex++] = genotype[1];
                            }
                        }

                        VariantRunData runToSave = addPlinkDataToVariant(mongoTemplate, variant, nAssemblyId, sequence, bpPosition, userIndividualToPopulationMap, alleles, project, sRun, previouslyCreatedSamples, fImportUnknownVariants);
                        
                        if (variant.getReferencePosition(nAssemblyId) != null && !project.getSequences(nAssemblyId).contains(variant.getReferencePosition(nAssemblyId).getSequence()))
                            project.getSequences(nAssemblyId).add(variant.getReferencePosition(nAssemblyId).getSequence());

                        project.getAlleleCounts().add(variant.getKnownAlleleList().size()); // it's a TreeSet so it will only be added if it's not already present
                        if (variant.getKnownAlleleList().size() > 2)
                            LOG.warn("Variant " + variant.getId() + " (" + providedVariantId + ") has more than 2 alleles!");

                        if (variant.getKnownAlleleList().size() > 0)
                        {   // we only import data related to a variant if we know its alleles
                            if (!unsavedVariants.contains(variant))
                                unsavedVariants.add(variant);
                            if (!unsavedRuns.contains(runToSave))
                                unsavedRuns.add(runToSave);
                        }

                        if (count == 0) {
                            nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / individuals.length);
                            LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);
                        }
                        else if (count % nNumberOfVariantsToSaveAtOnce == 0) {
                            saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, nNumberOfVariantsToSaveAtOnce, count, variantsAndPositions.size(), threadsToWaitFor, nNConcurrentThreads, chunkIndex++);
                            unsavedVariants = new HashSet<>();
                            unsavedRuns = new HashSet<>();
                        }
                    }
                    count++;
                }
                scanner.close();
                
                persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
                for (Thread t : threadsToWaitFor) // wait for all threads before moving to next phase
                    t.join();
            }

            // save project data
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);    // always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
            mongoTemplate.insert(previouslyCreatedSamples.values(), GenotypingSample.class);
        }
        finally
        {
            scanner.close();
            for (File f : tempFiles)
                if (f != null)
                    f.delete();
        }
        return count;
    }

    private File[] rotatePlinkPedFile(String[] variants, File pedFile, Map<String, String> userIndividualToPopulationMapToFill, ProgressIndicator progress) throws Exception
    {
        long before = System.currentTimeMillis();
        Runtime rt = Runtime.getRuntime();
        
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        boolean fCalledFromCommandLine = stacktrace[stacktrace.length-1].getClassName().equals(getClass().getName()) && "main".equals(stacktrace[stacktrace.length-1].getMethodName());
        
        // we grant ourselves a portion of the currently available memory for reading data: this defines how many markers we treat at once
        long allocatableMemory = (long) ((fCalledFromCommandLine ? .8 : .5) * (rt.maxMemory() - rt.totalMemory() + rt.freeMemory()));
        float readableFilePortion = (float) allocatableMemory / pedFile.length();
        int nMaxMarkersReadAtOnce = (int) (readableFilePortion * variants.length) / 4;
        int nCurrentChunkIndex = 0, nNumberOfChunks = (int) Math.ceil((float) variants.length / nMaxMarkersReadAtOnce);
        StringBuffer[] variantStringBuffers = null;  // one per variant
        
        File[] outputFiles = new File[nNumberOfChunks];
        List<Integer> readingPositions = new ArrayList<>();   // keep track of last read position on each individual's line
        try
        {
            while (nCurrentChunkIndex < nNumberOfChunks) {   // write set of files for current chunk
                int nMarkersReadAtOnce = nCurrentChunkIndex == nNumberOfChunks - 1 ? variants.length % nMaxMarkersReadAtOnce : nMaxMarkersReadAtOnce;
                
                String[] cleanLines = new String[Runtime.getRuntime().availableProcessors()];
                LinkedHashSet<Integer> linesContainingMultiBaseAlleles = new LinkedHashSet<>();
                List<Thread> lineCleaningThreads = new ArrayList<>();
                for (int i=0; i<cleanLines.length; i++)
                    lineCleaningThreads.add(null);

                if (nCurrentChunkIndex == 0)
                    variantStringBuffers = new StringBuffer[nMarkersReadAtOnce];

                outputFiles[nCurrentChunkIndex] = File.createTempFile(nCurrentChunkIndex + "-plinkImportVariantChunk-" + pedFile.getName() + "-", ".tsv");
                FileWriter fw = new FileWriter(outputFiles[nCurrentChunkIndex]);
                try
                {
                    for (int i=0; i<nMarkersReadAtOnce; i++)
                        variantStringBuffers[i] = new StringBuffer(variants[nCurrentChunkIndex*nMaxMarkersReadAtOnce + i]);

                    int nLineNumber = 0;
                    Scanner sc = new Scanner(pedFile);

                    while (sc.hasNextLine()) {
                        readingPositions.add(0);
                        int nFinalCurrentChunkIndex = nCurrentChunkIndex, nFinalLineNumber = nLineNumber;
                        String sLine = sc.nextLine();
                        Thread readingThread = new Thread() {
                            public void run() {
                                String sCleanLine = sLine.trim().replaceAll("[\t ]+", " ");

                                if (nFinalCurrentChunkIndex == 0) {
                                    PlinkEigenstratTool.readIndividualFromPlinkPedLine(sCleanLine, (HashMap<String, String>) userIndividualToPopulationMapToFill);   // important because it fills the map
                                    int nCurrentPos = 0;
                                    for (int nSpaceCount = 0; nSpaceCount < 6; nSpaceCount++)
                                        nCurrentPos = 1 + sCleanLine.indexOf(" ", nCurrentPos);
                                    readingPositions.set(nFinalLineNumber, nCurrentPos);
                                }
                                int threadIndex = nFinalLineNumber % cleanLines.length;
                                cleanLines[threadIndex] = sCleanLine.substring(readingPositions.get(nFinalLineNumber));
                                
                                if (nFinalCurrentChunkIndex == 0 && cleanLines[threadIndex].length() + 1 > 4 * variants.length)
                                    linesContainingMultiBaseAlleles.add(nFinalLineNumber);
                            }
                        };
                        readingThread.start();

                        int nThreadIndexForLastReadLine = nLineNumber % cleanLines.length;
                        lineCleaningThreads.set(nThreadIndexForLastReadLine, readingThread);

                        if (nThreadIndexForLastReadLine == cleanLines.length - 1 || !sc.hasNextLine()) {   // all threads have been launched: wait for termination then parse cleaned-up lines and write into appropriate buffers
                            for (int j=nLineNumber - nThreadIndexForLastReadLine; j<=nLineNumber; j++) {
                                int nThreadIndexToParse = j % cleanLines.length;
                                lineCleaningThreads.get(nThreadIndexToParse).join();
                                int nCurrentPos = 0, nPreviousPos, nReadMarkerCount = 0;
                                String sCleanLine = cleanLines[nThreadIndexToParse];
                                if (sCleanLine != null) {
                                    while (nReadMarkerCount < nMarkersReadAtOnce) {
                                        if (!linesContainingMultiBaseAlleles.contains(j))
                                            try {
                                                variantStringBuffers[nReadMarkerCount].append(" " + sCleanLine.charAt(nCurrentPos) + "/" + sCleanLine.charAt(nCurrentPos + 2));
                                                nCurrentPos += 4;
                                            }
                                            catch (StringIndexOutOfBoundsException sioobe) {
                                                throw new Exception("Line " + (j+1) + ": Number of variants does not match between .map (" + variants.length + ") and .ped (" + nReadMarkerCount + ") files!");
                                            }
                                        else
                                            for (int i=0; i<=1; i++) {  // 2 alleles per genotype
                                                nPreviousPos = nCurrentPos;
                                                nCurrentPos = 1 + sCleanLine.indexOf(" ", nCurrentPos);
                                                variantStringBuffers[nReadMarkerCount].append((i == 0 ? " " : "/") + (nCurrentPos == 0 ? sCleanLine.substring(nPreviousPos) : sCleanLine.substring(nPreviousPos, nCurrentPos - 1)));
                                            }
                                        nReadMarkerCount++;

                                        if (nCurrentPos == 0 && nReadMarkerCount < nMarkersReadAtOnce /* if it goes back to 0 before reaching nMarkersReadAtOnce it means the .map file contains more variants than the .ped file has genotypes per individual*/)
                                            throw new Exception("Line " + (j+1) + ": Number of variants does not match between .map (" + variants.length + ") and .ped (" + nReadMarkerCount + ") files!");
                                    }
                                    sCleanLine = null;  // otherwise we might consider the same line again when the total number of individuals is not proportional to the number of cores (and thus threads)
                                    readingPositions.set(j, readingPositions.get(j) + nCurrentPos);
                                }
                            }
                        }
                        nLineNumber++;
                    }

                    sc.close();
                    for (int i=0; i<nMarkersReadAtOnce; i++)
                        fw.write(variantStringBuffers[i].toString() + "\n");
                }
                finally
                {
                    fw.close();
                }

                if (nCurrentChunkIndex != nNumberOfChunks - 1)
                    LOG.debug("rotatePlinkPedFile treated " + ((nCurrentChunkIndex+1) * nMaxMarkersReadAtOnce) + " markers in " + (System.currentTimeMillis() - before) + "ms");
                nCurrentChunkIndex++;
                
                progress.setCurrentStepProgress(nCurrentChunkIndex * 100 / nNumberOfChunks);
            }
        }
        catch (Exception e)
        {
            for (File f : outputFiles)
                if (f != null)
                    f.delete();
            throw e;
        }
        LOG.info("PED matrix transposition took " + (System.currentTimeMillis() - before) + "ms for " + variants.length + " markers and " + userIndividualToPopulationMapToFill.size() + " individuals");
        return outputFiles;
    }

    /**
     * Adds the PLINK data to variant.
     * @param fImportUnknownVariants 
     */
    static private VariantRunData addPlinkDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId, String sequence, Long bpPos, Map<String, String> userIndividualToPopulationMap, String[][] alleles, GenotypingProject project, String runName, Map<String /*individual*/, GenotypingSample> usedSamples, boolean fImportUnknownVariants) throws Exception
    {
        if (fImportUnknownVariants && variantToFeed.getReferencePosition(nAssemblyId) == null && sequence != null)  // otherwise we leave it as it is (had some trouble with overridden end-sites)
            variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(sequence, bpPos, bpPos));

        VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
                
        // genotype fields
        int i = -1;
        HashMap<String, Integer> alleleIndexMap = new HashMap<>();  // should be more efficient not to call indexOf too often...
        for (String aKnownAllele : variantToFeed.getKnownAlleleList())
            alleleIndexMap.put(aKnownAllele, alleleIndexMap.size());
        List<Allele> knownAlleles = new ArrayList<>();
        for (String all : variantToFeed.getKnownAlleleList())
            knownAlleles.add(Allele.create(all, knownAlleles.isEmpty()));
        for (String sIndividual : userIndividualToPopulationMap.keySet())
        {
            i++;
            for (int j=0; j<=1; j++) { // 2 alleles per genotype
                Integer alleleIndex = alleleIndexMap.get(alleles[j][i]);
                if (alleleIndex == null && alleles[j][i].matches("[AaTtGgCc]+")) { // New allele
                    alleleIndex = variantToFeed.getKnownAlleleList().size();
                    variantToFeed.getKnownAlleleList().add(alleles[j][i]);
                    alleleIndexMap.put(alleles[j][i], alleleIndex);
                    knownAlleles.add(Allele.create(alleles[j][i], alleleIndex == 0));
                }
            }

            String gtCode = null;
            if (!"0".equals(alleles[0][i]) && !"0".equals(alleles[1][i]))
                try {
                    gtCode = Arrays.asList(alleles[0][i], alleles[1][i]).stream().map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/"));
                }
                catch (Exception e) {
                    LOG.warn("Ignoring invalid PLINK genotype \"" + alleles[0][i] + "/" + alleles[1][i] + "\" for variant " + variantToFeed.getId() + " and individual " + sIndividual);
                }

            if (gtCode == null)
                continue;   // we don't add missing genotypes
            
            SampleGenotype aGT = new SampleGenotype(gtCode);
            if (!usedSamples.containsKey(sIndividual))  // we don't want to persist each sample several times
            {
                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                boolean fAlreadyExists = ind != null, fNeedToSave = true;
                if (!fAlreadyExists)
                    ind = new Individual(sIndividual);
                String sPop = userIndividualToPopulationMap.get(sIndividual);
                if (!sPop.equals(".") && sPop.length() == 3)
                    ind.setPopulation(sPop);
                else if (!sIndividual.substring(0, 3).matches(".*\\d+.*") && sIndividual.substring(3).matches("\\d+"))
                    ind.setPopulation(sIndividual.substring(0, 3));
                else {
                    LOG.warn("Unable to find 3-letter population code for individual " + sIndividual);
                    if (fAlreadyExists)
                        fNeedToSave = false;
                }
                
                if (fNeedToSave)
                    mongoTemplate.save(ind);

                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                usedSamples.put(sIndividual, new GenotypingSample(sampleId, project.getId(), vrd.getRunName(), sIndividual));   // add a sample for this individual to the project
            }
        
            vrd.getSampleGenotypes().put(usedSamples.get(sIndividual).getId(), aGT);
        }
        
        // mandatory fields
        if (!knownAlleles.isEmpty()) {
            Type variantType = determineType(knownAlleles);
            if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType().toString())) {
                variantToFeed.setType(variantType.toString());
//                FIXME: also update existing VRDs
            }
            else if (!variantToFeed.getType().equals(variantType.toString()))
                throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());
        }
        
        vrd.setKnownAlleleList(variantToFeed.getKnownAlleleList());
        vrd.setReferencePositions(variantToFeed.getReferencePositions());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        return vrd;
    }
    
    /* FIXME: this mechanism could be improved to "fill holes" when genotypes are provided for some synonyms but not others (currently we import them all so the last encountered one "wins") */ 
    private HashMap<String, ArrayList<String>> checkSynonymGenotypeConsistency(File[] tempFiles, HashMap<String, String> existingVariantIDs, Collection<String> individualsInProvidedOrder, String outputPathAndPrefix) throws IOException, WrongNumberArgsException
    {
        long b4 = System.currentTimeMillis();
        LOG.info("Checking genotype consistency between synonyms...");
        String sLine = null;
        
        FileOutputStream inconsistencyFOS = new FileOutputStream(new File(outputPathAndPrefix + "-INCONSISTENCIES.txt"));
        HashMap<String /*existing variant id*/, ArrayList<String /*individual*/>> result = new HashMap<>();

        // first pass: identify synonym lines
        Map<String, List<Integer>> variantLinePositions = new HashMap<>();
        int nLinePerFileCount = 0, nCurrentLineGlobalPos = 0;
        for (int i=0; i<tempFiles.length; i++) {
            Scanner scanner = new Scanner(tempFiles[i]);
            try {
                while (scanner.hasNextLine()) {
                    sLine = scanner.nextLine();
                    String providedVariantName = sLine.substring(0, sLine.indexOf(" "));
                    String existingId = existingVariantIDs.get(providedVariantName.toUpperCase());
                    if (existingId != null && !existingId.toString().startsWith("*")) {
                        List<Integer> variantLines = variantLinePositions.get(existingId);
                        if (variantLines == null) {
                            variantLines = new ArrayList<>();
                            variantLinePositions.put(existingId, variantLines);
                        };
                        variantLines.add(nCurrentLineGlobalPos);
                    }
                    if (i == 0)
                        nLinePerFileCount++;
                    nCurrentLineGlobalPos++;
                }
            }
            finally {
                scanner.close();
            }
        }
        
        // only keep those with at least 2 synonyms
        Map<String /*variant id */, List<Integer> /*corresponding line positions*/> synonymLinePositions = variantLinePositions.entrySet().stream().filter(entry -> variantLinePositions.get(entry.getKey()).size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        variantLinePositions.clear();   // release memory as this object is not needed anymore
        
        // hold all lines that will need to be compared to any other(s) in a map that makes them accessible by their line number
        HashMap<Integer, String> linesNeedingComparison = new HashMap<>();      
        TreeSet<Integer> linesToReadForComparison = new TreeSet<>();
        synonymLinePositions.values().stream().forEach(varPositions -> linesToReadForComparison.addAll(varPositions));  // incrementally sorted line numbers, simplifies re-reading
        int nCurrentFileIndex = -1, nCurrentLinePos = 0;
        Scanner scanner = null;
        for (int nLinePos : linesToReadForComparison) {
            int nFileIndexToReadFrom = (int) nLinePos / nLinePerFileCount;
            if (nCurrentFileIndex != nFileIndexToReadFrom) {    // we need to switch to another temp file
                if (scanner != null)
                    scanner.close();
                scanner = new Scanner(tempFiles[nFileIndexToReadFrom]);
                nCurrentFileIndex = nFileIndexToReadFrom;
                nCurrentLinePos = nLinePerFileCount * nCurrentFileIndex;
            }
            while (nCurrentLinePos <= nLinePos) {
                sLine = scanner.nextLine();
                nCurrentLinePos++;
            }
            linesNeedingComparison.put(nLinePos, sLine/*.substring(1 + sLine.indexOf("\t"))*/);
        }

        // for each variant with at least two synonyms, build an array (one cell per individual) containing Sets of (distinct) encountered genotypes: inconsistencies are found where these Sets contain several items
        for (String variantId : synonymLinePositions.keySet()) {
            HashMap<String /*genotype*/, HashSet<String> /*synonyms*/>[] individualGenotypeListArray = new HashMap[individualsInProvidedOrder.size()];
            List<Integer> linesToCompareForVariant = synonymLinePositions.get(variantId);
            for (int nLineNumber=0; nLineNumber<linesToCompareForVariant.size(); nLineNumber++) {
                String[] synAndGenotypes = linesNeedingComparison.get(linesToCompareForVariant.get(nLineNumber)).split(" ");
                for (int individualIndex = 0; individualIndex<individualGenotypeListArray.length; individualIndex++) {
                    if (individualGenotypeListArray[individualIndex] == null)
                        individualGenotypeListArray[individualIndex] = new HashMap<>();
                    String genotype = synAndGenotypes[1 + individualIndex];
                    if (genotype.equals("0/0"))
                        continue;   // if genotype is unknown this should not keep us from considering others
                    HashSet<String> synonymsWithThisGenotype = individualGenotypeListArray[individualIndex].get(genotype);
                    if (synonymsWithThisGenotype == null) {
                        synonymsWithThisGenotype = new HashSet<>();
                        individualGenotypeListArray[individualIndex].put(genotype, synonymsWithThisGenotype);
                    }
                    synonymsWithThisGenotype.add(synAndGenotypes[0]);
                }
            }
            
            Iterator<String> indIt = individualsInProvidedOrder.iterator();
            int individualIndex = 0;
            while (indIt.hasNext()) {
                String ind = indIt.next();
                HashMap<String, HashSet<String>> individualGenotypes = individualGenotypeListArray[individualIndex++];
                if (individualGenotypes.size() > 1) {
                    ArrayList<String> individualsWithInconsistentGTs = result.get(variantId);
                    if (individualsWithInconsistentGTs == null) {
                        individualsWithInconsistentGTs = new ArrayList<String>();
                        result.put(variantId, individualsWithInconsistentGTs);
                    }
                    individualsWithInconsistentGTs.add(ind);
                    inconsistencyFOS.write(ind.getBytes());
                    for (String gt : individualGenotypes.keySet())
                        for (String syn : individualGenotypes.get(gt))
                            inconsistencyFOS.write(("\t" + syn + "=" + gt).getBytes());
                    inconsistencyFOS.write("\r\n".getBytes());
                }
            }
        }

        inconsistencyFOS.close();
        LOG.info("Inconsistency file was saved to " + outputPathAndPrefix + " in " + (System.currentTimeMillis() - b4) / 1000 + "s");
        return result;
    }
}