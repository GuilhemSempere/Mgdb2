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

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import fr.cirad.mgdb.importing.parameters.FlapjackImportParameters;
import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.importing.base.RefactoredImport;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.tools.ProgressIndicator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * The Class FlapjackImport.
 */
public class FlapjackImport extends RefactoredImport<FlapjackImportParameters> {

    /** The Constant LOG. */
    private static final Logger LOG = Logger.getLogger(VariantData.class);

    /** The m_process id. */
    //private String m_processID;

    public boolean m_fCloseContextOpenAfterImport = false;

    private static int m_nCurrentlyTransposingMatrixCount = 0;

    private File rotatedFile;

    ArrayList<String> individualNames = new ArrayList<>();

    /**
     * Instantiates a new Flapjack import.
     */
    public FlapjackImport()
    {
    }

    /**
     * Instantiates a new Flapjack import.
     *
     * @param processID the process id
     */
    public FlapjackImport(String processID)
    {
        m_processID = processID;
    }

    /**
     * Instantiates a new Flapjack import.
     */
    public FlapjackImport(boolean fCloseContextOpenAfterImport) {
        this();
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * Instantiates a new Flapjack import.
     */
    public FlapjackImport(String processID, boolean fCloseContextOpenAfterImport) {
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
        if (args.length < 7)
            throw new Exception("You must pass 7 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, MAP file, GENOTYPE file, and assembly name! An optional 8th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

        File mapFile = new File(args[4]);
        if (!mapFile.exists() || mapFile.length() == 0)
            throw new Exception("File " + args[4] + " is missing or empty!");

        File genotypeFile = new File(args[5]);
        if (!genotypeFile.exists() || genotypeFile.length() == 0)
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
        FlapjackImportParameters params = new FlapjackImportParameters(
                args[0], //sModule
                args[1], //sProject
                args[2], //sRun
                args[3], //sTechnology
                null, // nPloidy
                args[5], //assemblyName
                null, //sampleToIndividualMap
                false,//fSkipMonomorphic
                mode, //importMode
                (new File(args[4]).toURI().toURL()),
                new File(args[5])
        );
        new FlapjackImport().importToMongo(params);
    }

    @Override
    protected long doImport(FlapjackImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sModule = params.getsModule();
        String sProject = params.getsRun();
        String sRun = params.getsRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isfSkipMonomorphic();
        URL mapFileURL = params.getMainFileUrl();
        File genotypeFile = params.getGenotypeFile();
        Integer nPloidy = params.getnPloidy();
        int importMode = params.getImportMode();

        m_fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

        String info = "Loading variant list from MAP file";
        LOG.info(info);
        progress.addStep(info);
        progress.moveToNextStep();
        LinkedHashMap<String, String> variantsAndPositions = new LinkedHashMap<>();
        try {
            BufferedReader mapReader = new BufferedReader(new InputStreamReader(mapFileURL.openStream()));
            int nCurrentLine = -1;
            String line;
            while ((line = mapReader.readLine()) != null) {
                line = line.trim();
                nCurrentLine++;

                if (line.length() == 0 || line.charAt(0) == '#')
                    continue;

                String[] tokens = line.split("\\s+");
                if (tokens.length < 3)
                    throw new Exception("Line " + nCurrentLine + " : invalid or unsupported data (less than 3 elements)");

                variantsAndPositions.put(tokens[0], tokens[1] + "\t" + tokens[2]);
            }
            mapReader.close();
        } catch (Exception exc) {
            LOG.error(exc);
            progress.setError("Map file parsing failed : " + exc.getMessage());
            return 0;
        }


        // Rotate genotype matrix using temporary files
        info = "Reading and reorganizing genotypes";
        LOG.info(info);
        progress.addStep(info);
        progress.moveToNextStep();
        Map<String, Type> nonSnpVariantTypeMap = new HashMap<>();
        ArrayList<String> individualNames = new ArrayList<>();
        try {
            m_nCurrentlyTransposingMatrixCount++;
            nPloidy = transposeGenotypeFile(genotypeFile, rotatedFile, nPloidy, nonSnpVariantTypeMap, individualNames, fSkipMonomorphic, progress);
            if (importMode == 0 && createdProject == null && project.getPloidyLevel() != nPloidy)
                throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nPloidy + ") data!");
            project.setPloidyLevel(nPloidy);
        }
        finally {
            m_nCurrentlyTransposingMatrixCount--;
        }

        if (progress.getError() != null && !progress.isAborted())
            return 0;


        // Create the necessary samples
        LinkedHashMap<String, String> orderedIndOrSpToPopulationMap = new LinkedHashMap<>();
        for (String sInd : individualNames)
            orderedIndOrSpToPopulationMap.put(sInd, null);	// we have no population info
        //createSamples(mongoTemplate, project.getId(), sRun, sampleToIndividualMap, orderedIndOrSpToPopulationMap, progress);
        createCallSetsSamplesIndividuals(new ArrayList(orderedIndOrSpToPopulationMap.keySet()), mongoTemplate, project.getId(), sRun, sampleToIndividualMap, progress);
        if (progress.getError() != null || progress.isAborted())
            return 0;

        // Rotated file import
        int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nConcurrentThreads + " threads");
        long count = importTempFileContents(progress, nConcurrentThreads, mongoTemplate, assembly == null ? null : assembly.getId(), rotatedFile, variantsAndPositions, existingVariantIDs, project, sRun, null, orderedIndOrSpToPopulationMap, nonSnpVariantTypeMap, null, fSkipMonomorphic);
        if (progress.getError() != null)
            throw new Exception(progress.getError());

        if (progress.isAborted())
            return 0;

        return count;
    }


    @Override
    protected void initReader(FlapjackImportParameters params) throws Exception {
        rotatedFile = File.createTempFile("fjImport-" + params.getGenotypeFile().getName() + "-", ".tsv");
    }

    @Override
    protected void closeResource() throws IOException {
        rotatedFile.delete();
    }

    private long getAllocatableMemory(boolean fCalledFromCommandLine) {
        Runtime rt = Runtime.getRuntime();
        long allocatableMemory = (long) ((fCalledFromCommandLine ? .8 : .5) * (rt.maxMemory() - rt.totalMemory() + rt.freeMemory()));
        return allocatableMemory;
    }

    private static String insertMissingDashes(String originalGenotypeData) {
    	String result = null;
    	while ((result == null ? originalGenotypeData : result).contains("\t\t"))
    		result = (result == null ? originalGenotypeData : result).replaceAll("\t\t", "\t-\t");
    	
    	if (result == null)
    		result = originalGenotypeData;
    	
    	return result.endsWith("\t") ? result + "-" : result;
    }

    /**
     * 
     * @param genotypeFile
     * @param outputFile
     * @param nProvidedPloidy 
     * @param nonSnpVariantTypeMapToFill
     * @param individualListToFill
     * @param fSkipMonomorphic
     * @param progress
     * @return dataset's ploidy
     * @throws Exception
     */
    private int transposeGenotypeFile(File genotypeFile, File outputFile, Integer nProvidedPloidy, Map<String, Type> nonSnpVariantTypeMapToFill, ArrayList<String> individualListToFill, boolean fSkipMonomorphic, ProgressIndicator progress) throws Exception {
        long before = System.currentTimeMillis();

        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        boolean fCalledFromCommandLine = stacktrace[stacktrace.length-1].getClassName().equals(getClass().getName()) && "main".equals(stacktrace[stacktrace.length-1].getMethodName());

        int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());

        FileWriter outputWriter = new FileWriter(outputFile);

        Pattern allelePattern = Pattern.compile("\\S+");
        Pattern outputFileSeparatorPattern = Pattern.compile("(/|\\t)");

        HashSet<Integer> linesToIgnore = new HashSet<>();
        ArrayList<String> variants = new ArrayList<>();
        ArrayList<Integer> blockStartMarkers = new ArrayList<Integer>();  // blockStartMarkers[i] = first marker of block i
        ArrayList<ArrayList<Integer>> blockLinePositions = new ArrayList<ArrayList<Integer>>();  // blockLinePositions[line][block] = first character of `block` in `line`
        ArrayList<Integer> lineLengths = new ArrayList<Integer>();
        int maxLineLength = 0, maxPayloadLength = 0;
        blockStartMarkers.add(0);

        // Read the line headers, fill the individual map and creates the block positions arrays
        BufferedReader reader = new BufferedReader(new FileReader(genotypeFile));
        String initLine;
        int nIndividuals = 0, lineno = -1;
        while ((initLine = reader.readLine()) != null) {
            lineno += 1;
            if (initLine.trim().length() == 0 || initLine.charAt(0) == '#') {
                linesToIgnore.add(lineno);
                continue;
            }

            // Table header, with variant names, that starts with a tab (so the first non-whitespace word is not at index 0)
            if (variants.isEmpty()) {
            	if (nIndividuals > 0)
            		throw new Exception("Invalid individual name at line " + lineno);

            	for (String variantName : initLine.split("\\s+")) {
            		variantName = variantName.trim();
            		if (variantName.length() > 0)
            			variants.add(variantName);
            	}
            	linesToIgnore.add(lineno);
            }

            // Normal data line
            else {
                Matcher initMatcher = allelePattern.matcher(insertMissingDashes(initLine));
                initMatcher.find();
                String sIndividual = initMatcher.group().trim();

                individualListToFill.add(sIndividual);

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
        }
        reader.close();

        if (variants.size() == 0)
            throw new Exception("No variant names found, either the genotype matrix is empty, or the header line is missing or invalid");

        // Trivial case : each genotype is a collapsed homozygote SNP = 1 base + 1 separator, -1 because trailing separators are not accounted for
        final int nTrivialLineSize = 2*variants.size() - 1;
        final int initialCapacity = (int)((long)nIndividuals * (long)(2*maxPayloadLength - nTrivialLineSize + 1) / variants.size());  // +1 because of leading tabs
        final int maxBlockSize = (int)Math.ceil((float)variants.size() / nConcurrentThreads);
        LOG.debug(nIndividuals + " individuals, " + variants.size() + " variants, maxPayloadLength=" + maxPayloadLength + ", nTrivialLineSize=" + nTrivialLineSize);
        LOG.debug("Max line length : " + maxLineLength + ", initial capacity : " + initialCapacity);

        final int cMaxLineLength = maxLineLength;
        final int cIndividuals = nIndividuals;
        final int cVariants = variants.size();
        final AtomicInteger nFinishedVariantCount = new AtomicInteger(0), ploidy = new AtomicInteger(0);
        final AtomicLong memoryPool = new AtomicLong(0);
        Thread[] transposeThreads = new Thread[nConcurrentThreads];
        Type[] variantTypes = new Type[cVariants];
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

                        while (blockStartMarkers.get(blockStartMarkers.size() - 1) < cVariants && progress.getError() == null && !progress.isAborted()) {
                        	FileReader reader = new FileReader(genotypeFile);
                            try {
                                int blockIndex, blockSize, blockStart;
                                int bufferPosition = 0, bufferLength = 0;

                                // Only one import thread can allocate its memory at once
                                synchronized (AbstractGenotypeImport.class) {
                                    blockIndex = blockStartMarkers.size() - 1;
                                    blockStart = blockStartMarkers.get(blockStartMarkers.size() - 1);
                                    if (blockStart >= cVariants)
                                        return;

                                    // Take more memory if a significant amount has been released (e.g. when another import finished transposing)
                                    long allocatableMemory = getAllocatableMemory(fCalledFromCommandLine);
                                    if (allocatableMemory > memoryPool.get())
                                        memoryPool.set((allocatableMemory + memoryPool.get()) / 2);

                                    long blockGenotypesMemory = memoryPool.get() / nConcurrentThreads - cMaxLineLength;
                                    // max block size with the given amount of memory   | remaining variants to read
                                    blockSize = Math.min((int)(blockGenotypesMemory / (2*initialCapacity)), cVariants - blockStart);
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
                                for (int marker = 0; marker < blockSize; marker++)
                                    transposed.get(marker).setLength(0);

                                int individual = 0;
                                bufferLength = reader.read(fileBuffer, 0, cMaxLineLength);
                                for (int lineno = 0; lineno < cIndividuals + linesToIgnore.size(); lineno++) {
                                    // Read a line, but implementing the BufferedReader ourselves with our own buffers to avoid producing garbage
                                    lineBuffer.setLength(0);
                                    boolean reachedEOL = false;
                                    boolean ignore = linesToIgnore.contains(lineno);
                                    while (!reachedEOL) {
                                        for (int i = bufferPosition; i < bufferLength; i++) {
                                            if (fileBuffer[i] == '\n') {
                                                if (!ignore)
                                                    lineBuffer.append(fileBuffer, bufferPosition, i - bufferPosition);
                                                bufferPosition = i + 1;
                                                reachedEOL = true;
                                                break;
                                            }
                                        }

                                        if (!reachedEOL) {
                                            if (!ignore)
                                                lineBuffer.append(fileBuffer, bufferPosition, bufferLength - bufferPosition);
                                            if ((bufferLength = reader.read(fileBuffer, 0, cMaxLineLength)) < 0) {  // End of file
                                                break;
                                            }
                                            bufferPosition = 0;
                                        }
                                    }

                                    if (linesToIgnore.contains(lineno))
                                        continue;

                                    ArrayList<Integer> individualPositions = blockLinePositions.get(individual);

                                    // Trivial case : 1 character per genotype, 1 character per separator
                                    if (lineLengths.get(individual) == nTrivialLineSize) {
                                        for (int marker = 0; marker < blockSize; marker++) {
                                            int nCurrentPos = individualPositions.get(0) + 2*(blockStart + marker);
                                            char collapsedGenotype = lineBuffer.charAt(nCurrentPos);
//                                            if (collapsedGenotype == '-')
//                                                collapsedGenotype = '0';
                                            StringBuilder builder = transposed.get(marker);
                                            builder.append("\t");
                                            if (collapsedGenotype != '-')
                                            	builder.append(collapsedGenotype);
                                            //builder.append("/");
                                            //builder.append(collapsedGenotype);
                                        }
                                    // Non-trivial case : INDELs, heterozygotes and multi-characters separators
                                    } else {
                                        Matcher matcher = allelePattern.matcher(insertMissingDashes(lineBuffer.toString()));

                                        // Start at the closest previous block that has already been mapped
                                        int startBlock = Math.min(blockIndex, individualPositions.size() - 1);
                                        int startPosition = individualPositions.get(startBlock);

                                        // Advance till the beginning of the actual block, and map the other ones on the way
                                        matcher.find(startPosition);
                                        for (int b = startBlock; b < blockIndex; b++) {
                                            int nMarkersToSkip = blockStartMarkers.get(b+1) - blockStartMarkers.get(b);
                                            for (int i = 0; i < nMarkersToSkip; i++)
                                                matcher.find();

                                            // Need to synchronize structural changes
                                            synchronized (individualPositions) {
                                                if (individualPositions.size() <= b + 1)
                                                    individualPositions.add(matcher.start());
                                            }
                                        }

                                        for (int marker = 0; marker < blockSize; marker++) {
                                            StringBuilder builder = transposed.get(marker);
                                            String genotype = matcher.group();

                                            builder.append("\t");                                            
                                            if (!genotype.isEmpty() && !genotype.equals("-")) { // missing data as empty string
                                                if (nProvidedPloidy == null && genotype.contains("/")) {
                                                    int currentGtPloidy = genotype.split("/").length, currentProjectPloidy = ploidy.get();
                                                    if (currentProjectPloidy == 0) {
                                                        ploidy.set(currentGtPloidy);
                                                        LOG.info("Found ploidy level of " + ploidy.get() + " from genotype " + genotype);
                                                    }
                                                    else if (currentProjectPloidy != currentGtPloidy)
                                                        throw new Exception("Ambiguous ploidy level, please explicitly specify correct ploidy");
                                                }
                                                builder.append(genotype);
                                            }
                                            matcher.find();
                                        }

                                        // Map the current block
                                        synchronized (individualPositions) {
                                            if (individualPositions.size() <= blockIndex + 1 && blockStart + blockSize < cVariants)
                                                individualPositions.add(matcher.start());
                                        }
                                    }

                                    individual += 1;
                                }

                                for (int marker = 0; marker < blockSize; marker++) {
                                    String variantName = variants.get(blockStart + marker);
                                    String variantLine = transposed.get(marker).substring(1);  // Skip the leading tab

                                    // if it's not a SNP, let's keep track of its type
                                    List<Allele> alleleList =
                                            outputFileSeparatorPattern.splitAsStream(variantLine)
                                            	.filter(allele -> !allele.isEmpty())
                                                .distinct()
                                                .map(allele -> {
                                					try {
                                						return Allele.create(allele);
		                                            } catch (IllegalArgumentException e) {
		                                            	throw new IllegalArgumentException("Variant " + variantName + " - allele " + allele + " - "+ e.getClass().getName() + ": " + e.getMessage());
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

                                progress.setCurrentStepProgress(nFinishedVariantCount.addAndGet(blockSize) * 100 / cVariants);
                            } finally {
                                reader.close();
                            }
                        }
                    } catch (Throwable t) {
                        progress.setError("Genotype matrix transposition failed with error: " + t.getMessage());
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
            LOG.info("Genotype matrix transposition took " + (System.currentTimeMillis() - before) + "ms for " + cVariants + " markers and " + cIndividuals + " individuals");

            // Fill the variant type map with the variant type array
            for (int i = 0; i < cVariants; i++)
                if (variantTypes[i] != null)
                    nonSnpVariantTypeMapToFill.put(variants.get(i), variantTypes[i]);
        }

        Runtime.getRuntime().gc();  // Release our (lots of) memory as soon as possible
        return nProvidedPloidy != null ? nProvidedPloidy : ploidy.get();
    }
}