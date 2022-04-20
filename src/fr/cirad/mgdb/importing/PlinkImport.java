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
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

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
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.genotypes.PlinkEigenstratTool;
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

    public boolean m_fCloseContextOpenAfterImport = false;

    private static int m_nCurrentlyTransposingMatrixCount = 0;

    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport()
    {
    }

    /**
     * Instantiates a new PLINK import.
     *
     * @param processID the process id
     */
    public PlinkImport(String processID)
    {
        m_processID = processID;
    }

    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(boolean fCloseContextOpenAfterImport) {
        this();
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(String processID, boolean fCloseContextOpenAfterImport) {
        this(processID);
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception
    {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, MAP file, and PED file! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

        File mapFile = new File(args[4]);
        if (!mapFile.exists() || mapFile.length() == 0)
            throw new Exception("File " + args[4] + " is missing or empty!");

        File pedFile = new File(args[5]);
        if (!pedFile.exists() || pedFile.length() == 0)
            throw new Exception("File " + args[5] + " is missing or empty!");

        int mode = 0;
        try
        {
            mode = Integer.parseInt(args[6]);
        }
        catch (Exception e)
        {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        new PlinkImport().importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), new File(args[5]), false, true, mode);
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
     * @param fSkipMonomorphic whether or not to skip import of variants that have no polymorphism (where all individuals have the same genotype)
     * @param fCheckConsistencyBetweenSynonyms if set, will skip genotypes that are not consistent across provided synonyms
     * @param importMode the import mode
     * @return a project ID if it was created by this method, otherwise null
     * @throws Exception the exception
     */
    public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL mapFileURL, File pedFile, boolean fSkipMonomorphic, boolean fCheckConsistencyBetweenSynonyms, int importMode) throws Exception
    {
        if (m_nCurrentlyTransposingMatrixCount > 3) // we allow up to 4 simultaneous matrix rotations
            throw new Exception("The system is already busy rotating other PLINK datasets, please try again later");

        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"}); // better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
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

            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() != 2)
                throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + 2 + ") data!");

            MongoTemplateManager.lockProjectForWriting(sModule, sProject);

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

            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false);

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

            // rotate matrix using temporary files
            info = "Reading and reorganizing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            Map<String, String> userIndividualToPopulationMap = new LinkedHashMap<>();
            Map<String, Type> nonSnpVariantTypeMap = new HashMap<>();

            File rotatedFile = null;
            try {
                m_nCurrentlyTransposingMatrixCount++;
                rotatedFile = transposePlinkPedFile(variants, pedFile, userIndividualToPopulationMap, nonSnpVariantTypeMap, fSkipMonomorphic, progress);
            }
            finally {
                m_nCurrentlyTransposingMatrixCount--;
            }

            progress.addStep("Checking genotype consistency between synonyms");
            progress.moveToNextStep();

            HashMap<String, ArrayList<String>> inconsistencies = !fCheckConsistencyBetweenSynonyms ? null : checkSynonymGenotypeConsistency(rotatedFile, existingVariantIDs, userIndividualToPopulationMap.keySet(), pedFile.getParentFile() + File.separator + sModule + "_" + sProject + "_" + sRun);
            if (progress.getError() != null)
                return 0;

            int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nConcurrentThreads + " threads");
            long count = importTempFileContents(progress, nConcurrentThreads, mongoTemplate, rotatedFile, variantsAndPositions, existingVariantIDs, project, sRun, inconsistencies, userIndividualToPopulationMap, nonSnpVariantTypeMap, fSkipMonomorphic);

            if (progress.getError() != null)
                throw new Exception(progress.getError());

            progress.addStep("Preparing database for searches");
            progress.moveToNextStep();
            MgdbDao.prepareDatabaseForSearches(mongoTemplate);

            LOG.info("PlinkImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
            progress.markAsComplete();
            return createdProject;
        }
        finally
        {
            if (m_fCloseContextOpenAfterImport && ctx != null)
                ctx.close();
            MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
        }
    }

    public long importTempFileContents(ProgressIndicator progress, int nNConcurrentThreads, MongoTemplate mongoTemplate, File tempFile, LinkedHashMap<String, String> variantsAndPositions, HashMap<String, String> existingVariantIDs, GenotypingProject project, String sRun, HashMap<String, ArrayList<String>> inconsistencies, Map<String, String> userIndividualToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, boolean fSkipMonomorphic) throws Exception
    {
        String[] individuals = userIndividualToPopulationMap.keySet().toArray(new String[userIndividualToPopulationMap.size()]);
        final AtomicInteger count = new AtomicInteger(0);

        // loop over each variation and write to DB
        BufferedReader reader = null;
        try
        {
            String info = "Importing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            progress.setPercentageEnabled(true);

            final int nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / individuals.length);
            LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);

            // Create the necessary samples
            HashMap<String /*individual*/, GenotypingSample> samples = new HashMap<>();
            LinkedHashSet<String> individualsWithoutPopulation = new LinkedHashSet<>();
            for (String sIndividual : userIndividualToPopulationMap.keySet()) {
                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                boolean fAlreadyExists = ind != null;
                boolean fNeedToSave = true;
                if (!fAlreadyExists)
                    ind = new Individual(sIndividual);
                String sPop = userIndividualToPopulationMap.get(sIndividual);
                if (!sPop.equals(".") && sPop.length() == 3)
                    ind.setPopulation(sPop);
                else if (!sIndividual.substring(0, 3).matches(".*\\d+.*") && sIndividual.substring(3).matches("\\d+"))
                    ind.setPopulation(sIndividual.substring(0, 3));
                else {
                    individualsWithoutPopulation.add(sIndividual);
                    if (fAlreadyExists)
                        fNeedToSave = false;
                }

                if (fNeedToSave)
                    mongoTemplate.save(ind);

                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                samples.put(sIndividual, new GenotypingSample(sampleId, project.getId(), sRun, sIndividual));   // add a sample for this individual to the project
            }

            if (!individualsWithoutPopulation.isEmpty())
                LOG.warn("Unable to find 3-letter population code for individuals: " + StringUtils.join(individualsWithoutPopulation, ", "));

            reader = new BufferedReader(new FileReader(tempFile));

            final BufferedReader finalReader = reader;

            // Leave one thread dedicated to the saveChunk service, it looks empirically faster that way
            int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
            Thread[] importThreads = new Thread[nImportThreads];
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());

            for (int threadIndex = 0; threadIndex < nImportThreads; threadIndex++) {
                importThreads[threadIndex] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            long processedVariants = 0;
                            HashSet<VariantData> unsavedVariants = new HashSet<VariantData>();  // HashSet allows no duplicates
                            HashSet<VariantRunData> unsavedRuns = new HashSet<VariantRunData>();
                            while (progress.getError() == null && !progress.isAborted()) {
                                String line;
                                synchronized (finalReader) {
                                    line = finalReader.readLine();
                                }
                                if (line == null)
                                    break;
                                String[] splitLine = line.split("\t");

                                if (fSkipMonomorphic && Arrays.stream(splitLine, 1, splitLine.length).filter(gt -> !"0/0".equals(gt)).distinct().count() < 2)
                                    continue; // skip non-variant positions

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
                                Type type = nonSnpVariantTypeMap.get(providedVariantId);    // SNP is the default type so we don't store it in nonSnpVariantTypeMap to make it as lightweight as possible
                                for (String variantDescForPos : getIdentificationStrings(type == null ? Type.SNP.toString() : type.toString(), sequence, bpPosition, Arrays.asList(new String[] {providedVariantId}))) {
                                    variantId = existingVariantIDs.get(variantDescForPos);
                                    if (variantId != null) {
                                        if (type != null && !variantId.equals(providedVariantId))
                                            nonSnpVariantTypeMap.put(variantId, type);  // add the type to this existing variant ID so we don't miss it later on
                                        break;
                                    }
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

                                    VariantRunData runToSave = addPlinkDataToVariant(mongoTemplate, variant, sequence, bpPosition, userIndividualToPopulationMap, nonSnpVariantTypeMap, alleles, project, sRun, samples, fImportUnknownVariants);

                                    if (variant.getReferencePosition() != null)
                                        project.getSequences().add(variant.getReferencePosition().getSequence());

                                    project.getAlleleCounts().add(variant.getKnownAlleles().size()); // it's a TreeSet so it will only be added if it's not already present
                                    if (variant.getKnownAlleles().size() > 2)
                                        LOG.warn("Variant " + variant.getId() + " (" + providedVariantId + ") has more than 2 alleles!");

                                    if (variant.getKnownAlleles().size() > 0) {   // we only import data related to a variant if we know its alleles
                                        if (!unsavedVariants.contains(variant))
                                            unsavedVariants.add(variant);
                                        if (!unsavedRuns.contains(runToSave))
                                            unsavedRuns.add(runToSave);
                                    }
                                    else
                                    	LOG.warn("Skipping variant " + variant.getId() + " positioned at " + variant.getReferencePosition().getSequence() + ":" + variant.getReferencePosition().getStartSite() + " because its alleles are not known");

                                    if (processedVariants % nNumberOfVariantsToSaveAtOnce == 0) {
                                        saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, saveService);
                                        unsavedVariants = new HashSet<VariantData>();
                                        unsavedRuns = new HashSet<VariantRunData>();

                                        progress.setCurrentStepProgress(count.get() * 100 / variantsAndPositions.size());
                                    }
                                }
                                int newCount = count.incrementAndGet();
                                if (newCount % (nNumberOfVariantsToSaveAtOnce*50) == 0)
                                    LOG.debug(newCount + " lines processed");
                                processedVariants += 1;
                            }

                            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
                        } catch (Throwable t) {
                            progress.setError("Genotypes import failed with " + t.getClass().getSimpleName() + ": " + t.getMessage());
                            LOG.error(progress.getError(), t);
                            return;
                        }

                    }
                };

                importThreads[threadIndex].start();
            }

            for (int i = 0; i < nImportThreads; i++)
                importThreads[i].join();
            saveService.shutdown();
            saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

            if (progress.getError() != null || progress.isAborted())
                return 0;

            // save project data
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);    // always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
            mongoTemplate.insert(samples.values(), GenotypingSample.class);
        }
        finally
        {
            if (reader != null)
                reader.close();
            if (tempFile != null)
                tempFile.delete();
        }
        return count.get();
    }

    private long getAllocatableMemory(boolean fCalledFromCommandLine) {
        Runtime rt = Runtime.getRuntime();
        long allocatableMemory = (long) ((fCalledFromCommandLine ? .8 : .5) * (rt.maxMemory() - rt.totalMemory() + rt.freeMemory()));
        return allocatableMemory;
    }

    private File transposePlinkPedFile(String[] variants, File pedFile, Map<String, String> userIndividualToPopulationMapToFill, Map<String, Type> nonSnpVariantTypeMapToFill, boolean fSkipMonomorphic, ProgressIndicator progress) throws Exception {
        long before = System.currentTimeMillis();

        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        boolean fCalledFromCommandLine = stacktrace[stacktrace.length-1].getClassName().equals(getClass().getName()) && "main".equals(stacktrace[stacktrace.length-1].getMethodName());

        int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());

        File outputFile = File.createTempFile("plinkImport-" + pedFile.getName() + "-", ".tsv");
        FileWriter outputWriter = new FileWriter(outputFile);

        Pattern allelePattern = Pattern.compile("\\S+");
        Pattern outputFileSeparatorPattern = Pattern.compile("(/|\\t)");

        ArrayList<Integer> blockStartMarkers = new ArrayList<Integer>();  // blockStartMarkers[i] = first marker of block i
        ArrayList<ArrayList<Integer>> blockLinePositions = new ArrayList<ArrayList<Integer>>();  // blockLinePositions[line][block] = first character of `block` in `line`
        ArrayList<Integer> lineLengths = new ArrayList<Integer>();
        int maxLineLength = 0, maxPayloadLength = 0;
        blockStartMarkers.add(0);

        // Read the line headers, fill the individual map and creates the block positions arrays
        BufferedReader reader = new BufferedReader(new FileReader(pedFile));
        String initLine;
        int nIndividuals = 0;
        while ((initLine = reader.readLine()) != null) {
            Matcher initMatcher = allelePattern.matcher(initLine);
            initMatcher.find();
            String sPopulation = initMatcher.group();
            initMatcher.find();
            String sIndividual = initMatcher.group();
            userIndividualToPopulationMapToFill.put(sIndividual, sPopulation);

            // Skip the remaining header fields
            for (int i = 0; i < 4; i++)
                initMatcher.find();

            ArrayList<Integer> positions = new ArrayList<Integer>();

            // Find the first allele to get the actual beginning of the genotypes, without the first separators
            initMatcher.find();
            int payloadStart = initMatcher.start();
            positions.add(payloadStart);
            blockLinePositions.add(positions);

            // Find the length of the line's payload (without header and trailing separators)
            String sPayLoad = initLine.substring(payloadStart).trim();
            lineLengths.add(sPayLoad.length());

            if (initLine.length() > maxLineLength)
                maxLineLength = initLine.length();
            if (sPayLoad.length() > maxPayloadLength)
                maxPayloadLength = sPayLoad.length();

            nIndividuals += 1;
        }
        reader.close();

        // Counted as [allele, sep, allele, sep] : -1 because trailing separators are not accounted for
        final int nTrivialLineSize = 4*variants.length - 1;
        final int initialCapacity = (int)((long)nIndividuals * (long)(2*maxPayloadLength - nTrivialLineSize + 1) / variants.length);  // +1 because of leading tabs
        final int maxBlockSize = (int)Math.ceil((float)variants.length / nConcurrentThreads);
        LOG.debug(nIndividuals + " individuals, " + variants.length + " variants, maxPayloadLength=" + maxPayloadLength + ", nTrivialLineSize=" + nTrivialLineSize + " : " + (nIndividuals * (2*maxPayloadLength - nTrivialLineSize + 1) / variants.length));
        LOG.debug("Max line length : " + maxLineLength + ", initial capacity : " + initialCapacity);

        final int cMaxLineLength = maxLineLength;
        final int cIndividuals = nIndividuals;
        final AtomicInteger nFinishedVariantCount = new AtomicInteger(0);
        final AtomicLong memoryPool = new AtomicLong(0);
        Thread[] transposeThreads = new Thread[nConcurrentThreads];
        Type[] variantTypes = new Type[variants.length];
        Arrays.fill(variantTypes, null);

        for (int threadIndex = 0; threadIndex < nConcurrentThreads; threadIndex++) {
            final int cThreadIndex = threadIndex;
            transposeThreads[threadIndex] = new Thread() {
                @Override
                public void run() {
                    try {
                        // Those buffers have a fixed length, so they can be pre-allocated
                        StringBuilder lineBuffer = new StringBuilder(cMaxLineLength);
                        char[] fileBuffer = new char[cMaxLineLength];
                        ArrayList<StringBuilder> transposed = new ArrayList<StringBuilder>();

                        while (blockStartMarkers.get(blockStartMarkers.size() - 1) < variants.length && progress.getError() == null) {
                            FileReader reader = new FileReader(pedFile);
                            try {
                                int blockIndex, blockSize, blockStart;
                                int bufferPosition = 0, bufferLength = 0;

                                // Only one import thread can allocate its memory at once
                                synchronized (AbstractGenotypeImport.class) {
                                    blockIndex = blockStartMarkers.size() - 1;
                                    blockStart = blockStartMarkers.get(blockStartMarkers.size() - 1);
                                    if (blockStart >= variants.length)
                                        return;

                                    // Take more memory if a significant amount has been released (e.g. when another import finished transposing)
                                    long allocatableMemory = getAllocatableMemory(fCalledFromCommandLine);
                                    if (allocatableMemory > memoryPool.get())
                                        memoryPool.set((allocatableMemory + memoryPool.get()) / 2);

                                    long blockGenotypesMemory = memoryPool.get() / nConcurrentThreads - cMaxLineLength;
                                    //                   max block size with the given amount of memory   | remaining variants to read
                                    blockSize = Math.min((int)(blockGenotypesMemory / (2*initialCapacity)), variants.length - blockStart);
                                    blockSize = Math.min(blockSize, maxBlockSize);
                                    if (blockSize < 1)
                                        continue;

                                    blockStartMarkers.add(blockStart + blockSize);
                                    LOG.debug("Thread " + cThreadIndex + " starts block " + blockIndex + " : " + blockSize + " markers starting at marker " + blockStart + " (" + blockGenotypesMemory + " allowed)");


                                    if (transposed.size() < blockSize) {  // Allocate more buffers if needed
                                        transposed.ensureCapacity(blockSize);
                                        for (int i = transposed.size(); i < blockSize; i++) {
                                            transposed.add(new StringBuilder(initialCapacity));
                                        }
                                    }
                                }

                                // Reset the transposed variants buffers
                                for (int marker = 0; marker < blockSize; marker++) {
                                    transposed.get(marker).setLength(0);
                                }

                                bufferLength = reader.read(fileBuffer, 0, cMaxLineLength);
                                for (int individual = 0; individual < cIndividuals; individual++) {
                                    // Read a line, but implementing the BufferedReader ourselves with our own buffers to avoid producing garbage
                                    lineBuffer.setLength(0);
                                    boolean reachedEOL = false;
                                    while (!reachedEOL) {
                                        for (int i = bufferPosition; i < bufferLength; i++) {
                                            if (fileBuffer[i] == '\n') {
                                                lineBuffer.append(fileBuffer, bufferPosition, i - bufferPosition);
                                                bufferPosition = i + 1;
                                                reachedEOL = true;
                                                break;
                                            }
                                        }

                                        if (!reachedEOL) {
                                            lineBuffer.append(fileBuffer, bufferPosition, bufferLength - bufferPosition);
                                            if ((bufferLength = reader.read(fileBuffer, 0, cMaxLineLength)) < 0) {  // End of file
                                                break;
                                            }
                                            bufferPosition = 0;
                                        }
                                    }

                                    ArrayList<Integer> individualPositions = blockLinePositions.get(individual);

                                    // Trivial case : 1 character per allele, 1 character per separator
                                    if (lineLengths.get(individual) == nTrivialLineSize) {
                                        for (int marker = 0; marker < blockSize; marker++) {
                                            int nCurrentPos = individualPositions.get(0) + 4*(blockStart + marker);
                                            StringBuilder builder = transposed.get(marker);
                                            builder.append("\t");
                                            builder.append(lineBuffer.charAt(nCurrentPos));
                                            builder.append("/");
                                            builder.append(lineBuffer.charAt(nCurrentPos + 2));
                                        }
                                    // Non-trivial case : INDELs and/or multi-characters separators
                                    } else {
                                        Matcher matcher = allelePattern.matcher(lineBuffer);

                                        // Start at the closest previous block that has already been mapped
                                        int startBlock = Math.min(blockIndex, individualPositions.size() - 1);
                                        int startPosition = individualPositions.get(startBlock);

                                        // Advance till the beginning of the actual block, and map the other ones on the way
                                        matcher.find(startPosition);
                                        for (int b = startBlock; b < blockIndex; b++) {
                                            int nMarkersToSkip = blockStartMarkers.get(b+1) - blockStartMarkers.get(b);
                                            for (int i = 0; i < nMarkersToSkip; i++) {
                                                matcher.find();
                                                matcher.find();
                                            }

                                            // Need to synchronize structural changes
                                            synchronized (individualPositions) {
                                                if (individualPositions.size() <= b + 1)
                                                    individualPositions.add(matcher.start());
                                            }
                                        }

                                        for (int marker = 0; marker < blockSize; marker++) {
                                            StringBuilder builder = transposed.get(marker);
                                            builder.append("\t");
                                            builder.append(matcher.group());
                                            matcher.find();
                                            builder.append("/");
                                            builder.append(matcher.group());
                                            matcher.find();
                                        }

                                        // Map the current block
                                        synchronized (individualPositions) {
                                            if (individualPositions.size() <= blockIndex + 1 && blockStart + blockSize < variants.length)
                                                individualPositions.add(matcher.start());
                                        }
                                    }
                                }

                                for (int marker = 0; marker < blockSize; marker++) {
                                    String variantName = variants[blockStart + marker];
                                    String variantLine = transposed.get(marker).substring(1);  // Skip the leading tab

                                    // if it's not a SNP, let's keep track of its type
                                    List<Allele> alleleList =
                                            outputFileSeparatorPattern.splitAsStream(variantLine)
                                                .filter(allele -> !"0".equals(allele))
                                                .distinct()
                                                .map(allele -> Allele.create(allele))
                                                .collect(Collectors.toList());

                                    if (!alleleList.isEmpty()) {
                                        Type variantType = determineType(alleleList);
                                        if (variantType != Type.SNP) {
                                            variantTypes[blockStart + marker] = variantType;
                                        }
                                    }

                                    synchronized (outputWriter) {
                                        outputWriter.write(variantName);
                                        outputWriter.write("\t");
                                        outputWriter.write(variantLine);
                                        outputWriter.write("\n");
                                    }
                                }

                                progress.setCurrentStepProgress(nFinishedVariantCount.addAndGet(blockSize) * 100 / variants.length);
                            } finally {
                                reader.close();
                            }
                        }
                    } catch (Throwable t) {
                        progress.setError("PED matrix transposition failed with error: " + t.getMessage());
                        LOG.error(progress.getError(), t);
                        return;
                    }
                }
            };
            transposeThreads[threadIndex].start();
        }

        for (int i = 0; i < nConcurrentThreads; i++)
            transposeThreads[i].join();

        // Fill the variant type map with the variant type array
        for (int i = 0; i < variants.length; i++) {
            if (variantTypes[i] != null)
                nonSnpVariantTypeMapToFill.put(variants[i], variantTypes[i]);
        }
        outputWriter.close();
        if (progress.getError() == null)
            LOG.info("PED matrix transposition took " + (System.currentTimeMillis() - before) + "ms for " + variants.length + " markers and " + userIndividualToPopulationMapToFill.size() + " individuals");

        Runtime.getRuntime().gc();  // Release our (lots of) memory as soon as possible
        return outputFile;
    }

    /**
     * Adds the PLINK data to variant.
     * @param fImportUnknownVariants
     */
    static private VariantRunData addPlinkDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, String sequence, Long bpPos, Map<String, String> userIndividualToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, String[][] alleles, GenotypingProject project, String runName, Map<String /*individual*/, GenotypingSample> usedSamples, boolean fImportUnknownVariants) throws Exception
    {
        VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));

        // genotype fields
        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream().collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));  // should be more efficient not to call indexOf too often...
        int i = -1;
        for (String sIndividual : userIndividualToPopulationMap.keySet())
        {
            i++;

            if ("0".equals(alleles[0][i]) || "0".equals(alleles[1][i]))
                continue;  // Do not add missing genotypes

            for (int j = 0; j < 2; j++) { // 2 alleles per genotype
                Integer alleleIndex = alleleIndexMap.get(alleles[j][i]);
                if (alleleIndex == null && alleles[j][i].matches("[AaTtGgCc]+")) { // New allele
                    alleleIndex = variantToFeed.getKnownAlleles().size();
                    variantToFeed.getKnownAlleles().add(alleles[j][i]);
                    alleleIndexMap.put(alleles[j][i], alleleIndex);
                }
            }

            String gtCode;
            try {
                gtCode = Arrays.asList(alleles[0][i], alleles[1][i]).stream().map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/"));
            }
            catch (Exception e) {
                LOG.warn("Ignoring invalid PLINK genotype \"" + alleles[0][i] + "/" + alleles[1][i] + "\" for variant " + variantToFeed.getId() + " and individual " + sIndividual);
                continue;
            }

            /*if (gtCode == null)
                continue;   // we don't add missing genotypes*/

            SampleGenotype aGT = new SampleGenotype(gtCode);
            vrd.getSampleGenotypes().put(usedSamples.get(sIndividual).getId(), aGT);
        }
        
        if (fImportUnknownVariants && variantToFeed.getReferencePosition() == null && sequence != null) // otherwise we leave it as it is (had some trouble with overridden end-sites)
            variantToFeed.setReferencePosition(new ReferencePosition(sequence, bpPos, !variantToFeed.getKnownAlleles().isEmpty() ? bpPos + variantToFeed.getKnownAlleles().iterator().next().length() - 1 : null));

        // mandatory fields
        if (!alleleIndexMap.isEmpty()) {
            Type variantType = nonSnpVariantTypeMap.get(variantToFeed.getId());
            String sVariantType;
            if (variantType == null)
                sVariantType = Type.SNP.toString();
            else
                sVariantType = variantType.toString();

            if (variantToFeed.getType() == null || Type.NO_VARIATION == variantType) {
                variantToFeed.setType(sVariantType);
                project.getVariantTypes().add(sVariantType);
            }
            else if (!variantToFeed.getType().equals(sVariantType))
                throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());
        }

        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        return vrd;
    }

    /* FIXME: this mechanism could be improved to "fill holes" when genotypes are provided for some synonyms but not others (currently we import them all so the last encountered one "wins") */
    private HashMap<String, ArrayList<String>> checkSynonymGenotypeConsistency(File rotatedFile, HashMap<String, String> existingVariantIDs, Collection<String> individualsInProvidedOrder, String outputPathAndPrefix) throws IOException
    {
        long b4 = System.currentTimeMillis();
        LOG.info("Checking genotype consistency between synonyms...");
        String sLine = null;

        FileOutputStream inconsistencyFOS = new FileOutputStream(new File(outputPathAndPrefix + "-INCONSISTENCIES.txt"));
        HashMap<String /*existing variant id*/, ArrayList<String /*individual*/>> result = new HashMap<>();

        // first pass: identify synonym lines
        Map<String, List<Integer>> variantLinePositions = new HashMap<>();
        int nCurrentLinePos = 0;
        try (Scanner scanner = new Scanner(rotatedFile)) {
            while (scanner.hasNextLine()) {
                sLine = scanner.nextLine();
                String providedVariantName = sLine.substring(0, sLine.indexOf("\t"));
                String existingId = existingVariantIDs.get(providedVariantName.toUpperCase());
                if (existingId != null && !existingId.toString().startsWith("*")) {
                    List<Integer> variantLines = variantLinePositions.get(existingId);
                    if (variantLines == null) {
                        variantLines = new ArrayList<>();
                        variantLinePositions.put(existingId, variantLines);
                    };
                    variantLines.add(nCurrentLinePos);
                }
                nCurrentLinePos++;
            }
        }


        // only keep those with at least 2 synonyms
        Map<String /*variant id */, List<Integer> /*corresponding line positions*/> synonymLinePositions = variantLinePositions.entrySet().stream().filter(entry -> variantLinePositions.get(entry.getKey()).size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        variantLinePositions.clear();   // release memory as this object is not needed anymore

        // hold all lines that will need to be compared to any other(s) in a map that makes them accessible by their line number
        HashMap<Integer, String> linesNeedingComparison = new HashMap<>();
        TreeSet<Integer> linesToReadForComparison = new TreeSet<>();
        synonymLinePositions.values().stream().forEach(varPositions -> linesToReadForComparison.addAll(varPositions));  // incrementally sorted line numbers, simplifies re-reading
        nCurrentLinePos = 0;
        try (Scanner scanner = new Scanner(rotatedFile)) {
            for (int nLinePos : linesToReadForComparison) {
                while (nCurrentLinePos <= nLinePos) {
                    sLine = scanner.nextLine();
                    nCurrentLinePos++;
                }
                linesNeedingComparison.put(nLinePos, sLine/*.substring(1 + sLine.indexOf("\t"))*/);
            }
        }

        // for each variant with at least two synonyms, build an array (one cell per individual) containing Sets of (distinct) encountered genotypes: inconsistencies are found where these Sets contain several items
        for (String variantId : synonymLinePositions.keySet()) {
            HashMap<String /*genotype*/, HashSet<String> /*synonyms*/>[] individualGenotypeListArray = new HashMap[individualsInProvidedOrder.size()];
            List<Integer> linesToCompareForVariant = synonymLinePositions.get(variantId);
            for (int nLineNumber=0; nLineNumber<linesToCompareForVariant.size(); nLineNumber++) {
                String[] synAndGenotypes = linesNeedingComparison.get(linesToCompareForVariant.get(nLineNumber)).split("\t");
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
        LOG.info("Inconsistency file was saved to " + outputPathAndPrefix + "-INCONSISTENCIES.txt" + " in " + (System.currentTimeMillis() - b4) / 1000 + "s");
        return result;
    }
}