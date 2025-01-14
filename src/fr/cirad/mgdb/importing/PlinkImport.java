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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.importing.base.RefactoredImport;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
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
public class PlinkImport extends RefactoredImport {

    /** The Constant LOG. */
    private static final Logger LOG = Logger.getLogger(PlinkImport.class);

    /** The m_process id. */
    private String m_processID;

    public boolean m_fCloseContextOpenAfterImport = false;

    private static int m_nCurrentlyTransposingMatrixCount = 0;
    
    private static final Pattern nonWhiteSpaceBlockPattern = Pattern.compile("\\S+");
    private static final Pattern outputFileSeparatorPattern = Pattern.compile("(/|\\t)");

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
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }
    
    /**
     * Instantiates a new PLINK import.
     */
    public PlinkImport(String processID, boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }
    
    @Override
    protected boolean populationCodesExpected() {
		return true;
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
        PlinkImport instance = new PlinkImport();
        instance.setMaxExpectedAlleleCount(2);
        instance.importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), new File(args[5]), null, false, true, args[6], mode);
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
	 * @param sampleToIndividualMap the sample-individual mapping
     * @param fSkipMonomorphic whether or not to skip import of variants that have no polymorphism (where all individuals have the same genotype)
     * @param fCheckConsistencyBetweenSynonyms if set, will skip genotypes that are not consistent across provided synonyms
     * @param assemblyName the assembly name
     * @param importMode the import mode
     * @return a project ID if it was created by this method, otherwise null
     * @throws Exception the exception
     */
    public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL mapFileURL, File pedFile, HashMap<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, boolean fCheckConsistencyBetweenSynonyms, String assemblyName, int importMode) throws Exception
    {
        if (m_nCurrentlyTransposingMatrixCount > 3) // we allow up to 4 simultaneous matrix rotations
            throw new Exception("The system is already busy rotating other PLINK datasets, please try again later");

        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"}); // better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)

        GenericXmlApplicationContext ctx = null;
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

        m_fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);

        if (m_processID == null)
            m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

        GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
        if (importMode == 0 && project != null && project.getPloidyLevel() != 2)
            throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + 2 + ") data!");

        Integer createdProject = null;
        
        File rotatedFile = null;
        try
        {
            MongoTemplateManager.lockProjectForWriting(sModule, sProject);

            cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

            // create project if necessary
            if (project == null || importMode > 0) {   // create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
                project.setTechnology(sTechnology);
                if (importMode != 1)
                	createdProject = project.getId();
            }
            project.setPloidyLevel(2);

			progress.addStep("Scanning existing marker IDs");
			progress.moveToNextStep();
			Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
			HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

            String info = "Loading variant list from MAP file";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            LinkedHashSet<Integer> redundantVariantIndexes = new LinkedHashSet<>();
            LinkedHashMap<String, String> providedVariantPositions = PlinkEigenstratTool.getVariantsAndPositionsFromPlinkMapFile(mapFileURL, redundantVariantIndexes, "\t");
            String[] variants = providedVariantPositions.keySet().toArray(new String[providedVariantPositions.size()]);


            // Rotate matrix using temporary files
            info = "Reading and reorganizing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            Map<String, Type> nonSnpVariantTypeMap = new HashMap<>();
            LinkedHashMap<String, String> orderedIndOrSpToPopulationMap = new LinkedHashMap<>();
            try {
                m_nCurrentlyTransposingMatrixCount++;
                rotatedFile = transposePlinkPedFile(variants, pedFile, orderedIndOrSpToPopulationMap, nonSnpVariantTypeMap, fSkipMonomorphic, progress);
            }
            finally {
                m_nCurrentlyTransposingMatrixCount--;
            }
            
            
            // Create the necessary samples
            createSamples(mongoTemplate, project.getId(), sRun, sampleToIndividualMap, orderedIndOrSpToPopulationMap, progress);
            if (progress.getError() != null || progress.isAborted())
                return createdProject;
            
            
            // Consistency checking (optional)
            HashMap<String, ArrayList<String>> inconsistencies = null;
            HashSet<Integer> indexesOfLinesThatMustBeSkipped = new HashSet<>();
            if (fCheckConsistencyBetweenSynonyms) {
                progress.addStep("Checking genotype consistency between synonyms");
                progress.moveToNextStep();
                checkSynonymGenotypeConsistency(rotatedFile, existingVariantIDs, orderedIndOrSpToPopulationMap.keySet(), pedFile.getParentFile() + File.separator + sModule + "_" + sProject + "_" + sRun, indexesOfLinesThatMustBeSkipped);
            }
            if (progress.getError() != null || progress.isAborted())
                return createdProject;

            
            // Rotated file import
            int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nConcurrentThreads + " threads");

            long count = importTempFileContents(progress, nConcurrentThreads, mongoTemplate, assembly == null ? null : assembly.getId(), rotatedFile, providedVariantPositions, existingVariantIDs, project, sRun, inconsistencies, orderedIndOrSpToPopulationMap, nonSnpVariantTypeMap, indexesOfLinesThatMustBeSkipped, fSkipMonomorphic);

            if (progress.getError() != null)
                throw new Exception(progress.getError());

            if (!progress.isAborted())
            	LOG.info("PlinkImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
            return createdProject;
        }
        catch (Exception e) {
        	LOG.error("Error", e);
        	progress.setError(e.getMessage());
        	return createdProject;
        }
        finally  {
            if (m_fCloseContextOpenAfterImport && ctx != null)
                ctx.close();
            if (rotatedFile != null)
            	rotatedFile.delete();
            MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
            if (progress.getError() == null && !progress.isAborted()) {
                progress.addStep("Preparing database for searches");
                progress.moveToNextStep();
                MgdbDao.prepareDatabaseForSearches(sModule);
            }
        }
    }

    private long getAllocatableMemory(boolean fCalledFromCommandLine) {
        Runtime rt = Runtime.getRuntime();
        long allocatableMemory = (long) ((fCalledFromCommandLine ? .8 : .5) * (rt.maxMemory() - rt.totalMemory() + rt.freeMemory()));
        return allocatableMemory;
    }

    private File transposePlinkPedFile(String[] variants, File pedFile, Map<String, String> orderedIndOrSpToPopulationMapToFill, Map<String, Type> nonSnpVariantTypeMapToFill, boolean fSkipMonomorphic, ProgressIndicator progress) throws Exception {
        long before = System.currentTimeMillis();

        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        boolean fCalledFromCommandLine = stacktrace[stacktrace.length-1].getClassName().equals(getClass().getName()) && "main".equals(stacktrace[stacktrace.length-1].getMethodName());

        int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());

        File outputFile = File.createTempFile("plinkImport-" + pedFile.getName() + "-", ".tsv");
        FileWriter outputWriter = new FileWriter(outputFile);

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
            Matcher initMatcher = nonWhiteSpaceBlockPattern.matcher(initLine);
            initMatcher.find();
            String sPopulation = initMatcher.group();
            initMatcher.find();
            String sIndividual = initMatcher.group();
            orderedIndOrSpToPopulationMapToFill.put(sIndividual, sPopulation);

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

                        while (blockStartMarkers.get(blockStartMarkers.size() - 1) < variants.length && progress.getError() == null && !progress.isAborted()) {
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
                                            char firstAllele = lineBuffer.charAt(nCurrentPos);
                                            if (firstAllele != '0') {
	                                            builder.append(firstAllele);
	                                            builder.append("/");
	                                            builder.append(lineBuffer.charAt(nCurrentPos + 2));
                                            }
                                        }
                                    // Non-trivial case : INDELs and/or multi-characters separators
                                    } else {
                                        Matcher matcher = nonWhiteSpaceBlockPattern.matcher(lineBuffer);

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
                                            String sFirstAllele = matcher.group();
                                            matcher.find();
                                            if (!"0".equals(sFirstAllele)) {
                                            	builder.append(sFirstAllele);
	                                            builder.append("/");
	                                            builder.append(matcher.group());
                                            }
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
                                    List<Allele> alleleList = outputFileSeparatorPattern.splitAsStream(variantLine)
				                                                .filter(allele -> !allele.isEmpty())
				                                                .distinct()
				                                                .map(allele -> {
				                                                					try {
				                                                						return Allele.create(allele.equals("I") ? "NN" : (allele.equals("D") ? "N" : allele));
										                                            } catch (IllegalArgumentException e) {
										                                            	throw new IllegalArgumentException("Variant " + variantName + " - " + e.getClass().getName() + ": " + e.getMessage());
										                                            }
				                                                				})
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

        outputWriter.close();

        if (progress.getError() == null && !progress.isAborted()) {
        	LOG.info("PED matrix transposition took " + (System.currentTimeMillis() - before) + "ms for " + variants.length + " markers and " + orderedIndOrSpToPopulationMapToFill.size() + " individuals");

            // Fill the variant type map with the variant type array
            for (int i = 0; i < variants.length; i++)
                if (variantTypes[i] != null)
                    nonSnpVariantTypeMapToFill.put(variants[i], variantTypes[i]);
        }
        
        Runtime.getRuntime().gc();  // Release our (lots of) memory as soon as possible
        return outputFile;
    }
}