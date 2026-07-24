/** *****************************************************************************
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
 ******************************************************************************/
package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import com.opencsv.CSVReader;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;

public class IntertekImport extends AbstractGenotypeImport<FileImportParameters> {

    private static final Logger LOG = Logger.getLogger(VariantData.class);

    public boolean m_fCloseContextOpenAfterImport = false;

    final static protected String validAlleleRegex = "([\\*ATGC-]+|INS|DEL)".intern();

    public IntertekImport() {}

    public IntertekImport(String processID) {
        m_processID = processID;
    }

    public IntertekImport(boolean fCloseContextOpenAfterImport) {
        this();
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    public IntertekImport(String processID, boolean fCloseContextOpenAfterImport) {
        this(processID);
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, csv file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");
        }

        File csvFile = new File(args[4]);
        if (!csvFile.exists() || csvFile.length() == 0) {
            throw new Exception("File " + args[4] + " is missing or empty!");
        }

        int mode = 0;
        try {
            mode = Integer.parseInt(args[6]);
        } catch (Exception e) {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        FileImportParameters params = new FileImportParameters(
                args[0], //sModule
                args[1], //sProject
                args[2], //sRun
                args[3], //sTechnology
                null, // nPloidy
                args[5], //assemblyName
                null, //sampleToIndividualMap
                false,//fSkipMonomorphic
                mode,//importMode
                new File(args[4]).toURI().toURL()//mainFileUrl
        );
        new IntertekImport().importToMongo(params);
    }

    /**
     * Task class for dispatching variant processing
     */
    private static class VariantTask {
        public static final VariantTask POISON_PILL = new VariantTask(null, null, null);
        
        final String variantId;
        final HashMap<Integer, SampleGenotype> sampleGenotypes;
        final VariantData variant;
        
        VariantTask(String variantId, HashMap<Integer, SampleGenotype> sampleGenotypes, VariantData variant) {
            this.variantId = variantId;
            this.sampleGenotypes = sampleGenotypes;
            this.variant = variant;
        }
    }

    /**
     * Checks if a variant is monomorphic (all genotypes are the same or missing)
     */
    private boolean isMonomorphic(Map<Integer, SampleGenotype> sampleGenotypes) {
        String[] distinctGTs = sampleGenotypes.values().stream()
            .map(SampleGenotype::getCode)
            .filter(gtCode -> gtCode != null)
            .distinct()
            .toArray(String[]::new);
        
        return distinctGTs.length == 0 || 
               (distinctGTs.length == 1 && 
                Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2);
    }

    @Override
    protected long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sRun = params.getRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();
        URL fileURL = params.getMainFileUrl();

        final String[] snpHeader = {"SNPID","SNPNum","AlleleY","AlleleX","Sequence"};
        int snpColIndex = Arrays.asList(snpHeader).indexOf("SNPID");
        int yColIndex = Arrays.asList(snpHeader).indexOf("AlleleY");
        int xColIndex = Arrays.asList(snpHeader).indexOf("AlleleX");
        String[] limit = {"Scaling"};

        List<String> dataHeaderList = Arrays.asList(new String[]{"DaughterPlate","MasterPlate","MasterWell","Call","X","Y","SNPID","SubjectID","Norm","Carrier","DaughterWell","LongID"});

        int variantColIndex = dataHeaderList.indexOf("SNPID");
        int indColIndex = dataHeaderList.indexOf("SubjectID");
        int callColIndex = dataHeaderList.indexOf("Call");
        int xFIColIndex = dataHeaderList.indexOf("X");
        int yFIColIndex = dataHeaderList.indexOf("Y");
        int masterPlateColIndex = dataHeaderList.indexOf("MasterPlate");

        readAllSampleIDsToPreloadIndividuals(fileURL, dataHeaderList, indColIndex, progress);

        Set<String> variantIdsToSave = new HashSet<>();
        HashMap<String, VariantData> variants = new HashMap<>();
        HashMap<String, Map<String, String>> variantAllelesMap = new HashMap<>();
        m_providedIdToSampleMap = new HashMap<>();
        m_providedIdToCallsetMap = new HashMap<>();
        boolean fDbAlreadyContainedIndividuals = mongoTemplate.findOne(new Query(), Individual.class) != null;
        boolean fDbAlreadyContainedSamples = mongoTemplate.findOne(new Query(), GenotypingSample.class) != null;

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());
        Set<String> existingIds = new HashSet<>();
        existingIds.addAll(existingVariantIDs.values());

        final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
        if (assemblyIDs.isEmpty())
            assemblyIDs.add(null);

        Set<Individual> indsToAdd = new HashSet<>();
        Set<GenotypingSample> samplesToAdd = new HashSet<>(), samplesToUpdate = new HashSet<>();

        // --- DISPATCHER + QUEUE IMPLEMENTATION ---
        
        int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
        @SuppressWarnings("unchecked")
        BlockingQueue<VariantTask>[] workerQueues = new BlockingQueue[nImportThreads];
        for (int i = 0; i < nImportThreads; i++) {
            workerQueues[i] = new LinkedBlockingQueue<>();
        }

        BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
        ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
        
        final GenotypingProject finalProject = project;
        final MongoTemplate finalMongoTemplate = mongoTemplate;
        final Assembly finalAssembly = assembly;
        String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
        AtomicInteger totalProcessedVariantCount = new AtomicInteger(0);

        // Start workers
        Thread[] importThreads = new Thread[nImportThreads];
        for (int threadIndex = 0; threadIndex < nImportThreads; threadIndex++) {
            final int workerIndex = threadIndex;
            importThreads[threadIndex] = new Thread() {
                @Override
                public void run() {
                    try {
                        processVariantTasks(
                            workerQueues[workerIndex],
                            finalMongoTemplate,
                            finalAssembly == null ? null : finalAssembly.getId(),
                            finalProject,
                            sRun,
                            assemblyIDs,
                            progress,
                            saveService,
                            existingVariantIDs
                        );
                    } catch (Throwable t) {
                        progress.setError("Worker " + workerIndex + " failed: " + t.getMessage());
                        LOG.error(progress.getError(), t);
                    }
                }
            };
            importThreads[threadIndex].start();
        }

        // Reading csv file and dispatching - runs in main thread
        int count = 0;
        String currentVariantId = null;
        HashMap<Integer, SampleGenotype> sampleGenotypes = new HashMap<>();
        int nPloidy = 0;
        List<String> ambiguousVariants = new ArrayList<>();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(fileURL.openStream())); 
             CSVReader csvReader = new CSVReader(in)) {
            
            boolean snpPart = false;
            boolean dataPart = false;
            String[] values;
            int i = 0;

            while ((values = csvReader.readNext()) != null) {
                if (progress.getError() != null || progress.isAborted())
                    return 0;

                i = i + 1;
                if (Arrays.asList(values).containsAll(Arrays.asList(snpHeader))) {
                    snpPart = true;
                    continue;
                }
                if (Arrays.asList(values).containsAll(Arrays.asList(limit))) {
                    snpPart = false;
                    continue;
                }

                // Reading Variants Part
                if (snpPart && !dataPart && !values[0].equals("")) {
                    String providedVariantId = values[snpColIndex];
                    
                    // --- SIMPLE VARIANT RESOLUTION (ID-based only, no position) ---
                    String variantId = null;
                    boolean hasValidId = providedVariantId != null && !providedVariantId.isEmpty() && !".".equals(providedVariantId);
                    
                    // Try to find by ID only (Intertek has no position in variant header)
                    if (hasValidId) {
                        variantId = existingVariantIDs.get(providedVariantId.toUpperCase());
                    }
                    
                    VariantData variant = null;
                    if (variantId != null) {
                        variant = mongoTemplate.findById(variantId, VariantData.class);
                    }
                    
                    Map<String, String> allelesMap = new HashMap<>();
                    
                    if (variant == null) {
                        // Create new variant
                        if (hasValidId) {
                            variantId = (ObjectId.isValid(providedVariantId) ? "_" : "") + providedVariantId;
                        } else {
                            variantId = generatedIdBaseString + String.format("%09x", totalProcessedVariantCount.getAndIncrement());
                        }
                        variant = new VariantData(variantId);
                        
                        // Parse alleles
                        String ref = values[xColIndex];
                        String alt = values[yColIndex];
                                                
                        if (ref.equals(alt))
                            throw new Exception("Identical AlleleX and AlleleY alleles '" + ref + "' provided for variant " + providedVariantId);
                        if (!ref.matches(validAlleleRegex) && !alt.matches("INS") && !alt.matches("DEL"))
                            throw new Exception("Invalid AlleleX allele '" + ref + "' provided for variant " + providedVariantId);
                        if (!alt.matches(validAlleleRegex) && !ref.matches("INS") && !ref.matches("DEL"))
                            throw new Exception("Invalid AlleleY allele '" + alt + "' provided for variant " + providedVariantId);

                        if ((ref.equals("-") && (alt.matches("[ATGCatgc]+") || alt.equalsIgnoreCase("INS")) || (ref.equalsIgnoreCase("DEL")))) {
                            ref = "N";
                            alt = "NN";
                            variant.setType(Type.INDEL.toString());
                        } else if ((alt.equals("-") && (ref.matches("[ATGCatgc]+") || ref.equalsIgnoreCase("INS")) || (alt.equalsIgnoreCase("DEL")))) {
                            ref = "NN";
                            alt = "N";
                            variant.setType(Type.INDEL.toString());
                        } else {
                            variant.setType(Type.SNP.toString());
                        }

                        variant.getKnownAlleles().add(ref);
                        variant.getKnownAlleles().add(alt);
                        allelesMap.put(values[xColIndex], "0");
                        allelesMap.put(values[yColIndex], "1");
                        variantIdsToSave.add(providedVariantId);
                    } else {
                        // Match Intertek alleles to VCF format
                        String ref = variant.getKnownAlleles().get(0);
                        List<String> altList = variant.getKnownAlleles().subList(1, variant.getKnownAlleles().size());
                        String alleleX = values[xColIndex];
                        String alleleY = values[yColIndex];
                        
                        if (variant.getType().equals(Type.SNP.toString())) {
                            if (reverseComplement(alleleX).equals(alleleY)) {
                                // Ambiguity, can't know which one is ref. So arbitrary, X=ref and Y=alt
                                int posX = altList.indexOf(alleleX);
                                int posY = altList.indexOf(alleleY);
                                if ((alleleX.equals(ref) && posY != -1) || (posX != -1 && alleleY.equals(ref))) {
                                    allelesMap.put(alleleX, "0");
                                    allelesMap.put(alleleY, posY != -1 ? String.valueOf(posY + 1) : String.valueOf(posX + 1));
                                    ambiguousVariants.add(providedVariantId);
                                } else {
                                    throw new Exception("Provided AlleleX / AlleleY (" + alleleX + "/" + alleleY + ") for variant " + providedVariantId + " don't match with stored REF/ALT alleles " + ref + "/" + String.join("/", altList));
                                }
                            } else {
                                // See if ref is equals to X or Y or their reverse-complement
                                int posX = altList.indexOf(alleleX);
                                int posY = altList.indexOf(alleleY);
                                int posRCX = altList.indexOf(reverseComplement(alleleX));
                                int posRCY = altList.indexOf(reverseComplement(alleleY));

                                if (alleleX.equals(ref) && posY != -1 || reverseComplement(alleleX).equals(ref) && posRCY != -1) {
                                    allelesMap.put(alleleX, "0");
                                    allelesMap.put(alleleY, posY != -1 ? String.valueOf(posY + 1) : String.valueOf(posRCY + 1));
                                } else if (posX != -1 && alleleY.equals(ref) || posRCX != -1 && reverseComplement(alleleY).equals(ref)) {
                                    allelesMap.put(alleleX, posX != -1 ? String.valueOf(posX) : String.valueOf(posRCX + 1));
                                    allelesMap.put(alleleY, "0");
                                } else {
                                    throw new Exception("Provided AlleleX / AlleleY (" + alleleX + "/" + alleleY + ") for variant " + providedVariantId + " don't match with stored REF/ALT alleles " + ref + "/" + String.join("/", altList));
                                }
                            }
                        } else if (variant.getType().equals(Type.INDEL.toString())) {
                            String indelPart = null;
                            // Get inserted or deleted part
                            for (int a = 0; a < altList.size(); a++) {
                                String alt = altList.get(a);
                                if (alt.startsWith(ref))
                                    indelPart = alt.substring(ref.length()); // insertion after
                                else if (alt.endsWith(ref))
                                    indelPart = alt.substring(0, alt.length() - ref.length()); // insertion before

                                if (indelPart != null) { // insertion
                                    if (alleleY.equals("-") && (indelPart.equals(alleleX) || reverseComplement(indelPart).equals(alleleX))) {
                                        allelesMap.put(alleleX, String.valueOf(a + 1));
                                        allelesMap.put(alleleY, "0");
                                        break;
                                    } else if (alleleX.equals("-") && (indelPart.equals(alleleY) || reverseComplement(indelPart).equals(alleleY))) {
                                        allelesMap.put(alleleX, "0");
                                        allelesMap.put(alleleY, String.valueOf(a + 1));
                                        break;
                                    }
                                } else if (ref.startsWith(alt)) { // deletion after
                                    indelPart = ref.substring(alt.length());
                                } else if (ref.endsWith(alt)) { // deletion before
                                    indelPart = ref.substring(0, ref.length() - alt.length());
                                }

                                if (indelPart != null) { // deletion
                                    if (values[yColIndex].equals("-")) {
                                        allelesMap.put(alleleX, "0");
                                        allelesMap.put(alleleY, String.valueOf(a + 1));
                                    } else if (values[xColIndex].equals("-")) {
                                        allelesMap.put(alleleX, String.valueOf(a + 1));
                                        allelesMap.put(alleleY, "0");
                                    }
                                }
                            }
                            if (indelPart == null) {
                                throw new Exception("Given alleleX/alleleY (" + alleleX + "/" + alleleY + ") for variant " + providedVariantId + " don't match with stored REF/ALT alleles " + ref + "/" + String.join("/", altList));
                            }
                        }
                    }
                    
                    project.getVariantTypes().add(variant.getType());
                    variants.put(providedVariantId, variant);
                    variantAllelesMap.put(providedVariantId, allelesMap);
                    project.getAlleleCounts().add(variant.getKnownAlleles().size());
                    continue;
                }

                // Data part
                if (Arrays.asList(values).containsAll(dataHeaderList)) {
                    dataPart = true;
                    continue;
                }
                if (dataPart) {
                    String variantId = values[variantColIndex];
                    String bioEntityID = values[indColIndex];
                    String masterPlate = values[masterPlateColIndex];
                    String call = values[callColIndex];
                    String FI = values[yFIColIndex] + "," + values[xFIColIndex];

                    if (variantId.equals("") || bioEntityID.equals(""))
                        continue;

                    if (currentVariantId == null) {
                        currentVariantId = variantId;
                    }

                    if (!variantId.equals(currentVariantId)) {
                        // Check if we should skip monomorphic variants
                        boolean shouldSkip = false;
                        if (fSkipMonomorphic && !existingIds.contains(currentVariantId)) {
                            shouldSkip = isMonomorphic(sampleGenotypes);
                            if (shouldSkip) {
                                variantIdsToSave.remove(currentVariantId);
                            }
                        }
                        
                        // Only dispatch if not skipping
                        if (!shouldSkip) {
                            VariantData variant = variants.get(currentVariantId);
                            if (variant != null) {
                                int workerIndex = Math.floorMod(currentVariantId.hashCode(), nImportThreads);
                                VariantTask task = new VariantTask(currentVariantId, new HashMap<>(sampleGenotypes), variant);
                                workerQueues[workerIndex].put(task);
                            }
                        }
                        
                        sampleGenotypes = new HashMap<>();
                    }
                    currentVariantId = variantId;

                    String gtCode = null;
                    Map<String, String> variantAlleles = variantAllelesMap.get(variantId);
                    if (!call.equals("NTC")) {
                        if (call.contains(":")) {
                            List<String> alleles = Arrays.asList(call.split(":"));
                            gtCode = alleles.stream()
                                    .map(al -> variantAlleles.get(al))
                                    .collect(Collectors.joining("/"));
                            if (nPloidy == 0) {
                                nPloidy = alleles.size();
                                project.setPloidyLevel(nPloidy);
                            } else if (nPloidy != alleles.size()) {
                                throw new Exception("Ploidy levels differ between variants");
                            }
                        }
                        
                        GenotypingSample sample = null;
                        if (sampleToIndividualMap != null) {
                            if (fDbAlreadyContainedSamples) {
                                sample = mongoTemplate.findById(bioEntityID, GenotypingSample.class);
                                if (sample != null && !sampleToIndividualMap.isEmpty()) {
                                    String sProvidedIndividualForThisSample = determineIndividualNameAccountingForBrapiRelationships(sampleToIndividualMap, bioEntityID, progress);
                                    if (sProvidedIndividualForThisSample != null && !sample.getIndividual().equals(sProvidedIndividualForThisSample))
                                        throw new Exception("Sample " + bioEntityID + " already exists and is attached to individual " + sample.getIndividual() + ", not " + sProvidedIndividualForThisSample);
                                }
                            }
                            if (sample == null) {
                                String sIndividual = determineIndividualNameAccountingForBrapiRelationships(sampleToIndividualMap, bioEntityID, progress);
                                if (sIndividual == null)
                                    throw new Exception("Unable to determine individual for sample " + bioEntityID);

                                sample = new GenotypingSample(bioEntityID, sIndividual);
                                sample.getAdditionalInfo().put("masterPlate", masterPlate);
                                samplesToAdd.add(sample);
                            } else {
                                samplesToUpdate.add(sample);
                            }
                        } else {
                            sample = new GenotypingSample(bioEntityID + "-" + project.getId() + "-" + sRun, bioEntityID);
                            sample.getAdditionalInfo().put("masterPlate", masterPlate);
                            samplesToAdd.add(sample);
                        }

                        if (!fDbAlreadyContainedIndividuals || mongoTemplate.findById(sample.getIndividual(), Individual.class) == null)
                            indsToAdd.add(new Individual(sample.getIndividual()));

                        m_providedIdToSampleMap.put(bioEntityID, sample);
                        if (m_providedIdToCallsetMap.get(bioEntityID) == null) {
                            int callsetId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(Callset.class));
                            Callset cs = new Callset(callsetId, sample, project.getId(), sRun);
                            sample.getCallSets().add(cs);
                            m_providedIdToCallsetMap.put(bioEntityID, cs);
                        }

                        SampleGenotype sampleGt = new SampleGenotype(gtCode);
                        sampleGt.getAdditionalInfo().put(AbstractVariantData.GT_FIELD_FI, FI);
                        sampleGenotypes.put(m_providedIdToCallsetMap.get(bioEntityID).getId(), sampleGt);
                    }
                }
            }
            
            // Dispatch last variant
            if (currentVariantId != null) {
                boolean shouldSkip = false;
                if (fSkipMonomorphic && !existingIds.contains(currentVariantId)) {
                    shouldSkip = isMonomorphic(sampleGenotypes);
                    if (shouldSkip) {
                        variantIdsToSave.remove(currentVariantId);
                    }
                }
                
                if (!shouldSkip) {
                    VariantData variant = variants.get(currentVariantId);
                    if (variant != null) {
                        int workerIndex = Math.floorMod(currentVariantId.hashCode(), nImportThreads);
                        VariantTask task = new VariantTask(currentVariantId, new HashMap<>(sampleGenotypes), variant);
                        workerQueues[workerIndex].put(task);
                    }
                }
            }
            
            // Send poison pills
            for (BlockingQueue<VariantTask> queue : workerQueues) {
                queue.put(VariantTask.POISON_PILL);
            }

            // Wait for workers to complete
            for (Thread t : importThreads) {
                t.join();
            }

            saveService.shutdown();
            saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

            if (!ambiguousVariants.isEmpty()) {
                progress.markAsComplete("WARNING : Ambiguous matching between alleleX/alleleY and existing variant REF/ALT alleles for variants: " + String.join(",", ambiguousVariants));
            }

        }

        // Insert new callsets, samples and individuals
        insertNewCallSetsSamplesIndividuals(mongoTemplate, indsToAdd, samplesToAdd, samplesToUpdate);
        setSamplesPersisted(true);

        VCFFormatHeaderLine headerLineGT = new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
        VCFFormatHeaderLine headerLineFI = new VCFFormatHeaderLine(AbstractVariantData.GT_FIELD_FI, 2, VCFHeaderLineType.Float, "Fluorescence intensity");
        VCFHeader header = new VCFHeader(new HashSet<>(Arrays.asList(headerLineGT, headerLineFI)));
        mongoTemplate.save(new DBVCFHeader(new DBVCFHeader.VcfHeaderId(project.getId(), sRun), header));
        return count;
    }

    /**
     * Worker method that processes variant tasks with caching
     */
    private void processVariantTasks(
            BlockingQueue<VariantTask> queue,
            MongoTemplate mongoTemplate,
            Integer nAssemblyId,
            GenotypingProject project,
            String sRun,
            Collection<Integer> assemblyIDs,
            ProgressIndicator progress,
            ExecutorService saveService,
            HashMap<String, String> existingVariantIDs) throws Exception {
        
        HashSet<VariantData> unsavedVariants = new HashSet<>();
        HashSet<VariantRunData> unsavedRuns = new HashSet<>();
        HashMap<String, VariantData> variantCache = new HashMap<>();
        
        int chunkSize = Math.max(1, nMaxChunkSize / Math.max(1, m_providedIdToCallsetMap.size()));
        long processedVariants = 0;
        int localChunkSize = chunkSize;
        
        while (true) {
            VariantTask task = queue.take();
            if (task == VariantTask.POISON_PILL) {
                break;
            }
            
            String variantId = task.variantId;
            
            // Get or load variant from cache
            VariantData variant = variantCache.get(variantId);
            if (variant == null) {
                variant = mongoTemplate.findById(variantId, VariantData.class);
                if (variant == null) {
                    variant = task.variant;
                }
                variantCache.put(variantId, variant);
            }
            
            // Add run to variant
            variant.getRuns().add(new Run(project.getId(), sRun));
            
            // Create VariantRunData
            VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), sRun, variantId));
            vrd.setKnownAlleles(variant.getKnownAlleles());
            vrd.setSampleGenotypes(task.sampleGenotypes);
            vrd.setType(variant.getType());
            vrd.setPositions(variant.getPositions());
            vrd.setReferencePosition(variant.getReferencePosition());
            vrd.setSynonyms(variant.getSynonyms());
            
            // Track the variant
            if (variant.getKnownAlleles().size() > 0) {
                if (!unsavedVariants.contains(variant)) {
                    unsavedVariants.add(variant);
                }
                if (!unsavedRuns.contains(vrd)) {
                    unsavedRuns.add(vrd);
                }
                
                for (Integer asmId : assemblyIDs) {
                    ReferencePosition rp = variant.getReferencePosition(asmId);
                    project.getContigs(asmId).add(rp == null ? "" : rp.getSequence());
                }
                project.getVariantTypes().add(variant.getType());
                project.getAlleleCounts().add(variant.getKnownAlleles().size());
            }
            
            processedVariants++;
            
            if (processedVariants % localChunkSize == 0) {
                saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, saveService);
                
                variantCache.clear();
                unsavedVariants = new HashSet<>();
                unsavedRuns = new HashSet<>();
                
                progress.setCurrentStepProgress((int) processedVariants);
            }
            
            if (processedVariants % (localChunkSize * 50) == 0) {
                LOG.debug(processedVariants + " lines processed by worker");
            }
        }
        
        if (!unsavedVariants.isEmpty()) {
            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
        }
    }

    @Override
    protected void initReader(FileImportParameters params) throws Exception {}

    @Override
    protected void closeResource() throws IOException {}

    private void readAllSampleIDsToPreloadIndividuals(URL fileURL, List<String> dataHeaderList, int subjectColIndex, ProgressIndicator progress) throws Exception {
        Scanner scanner = new Scanner(new File(fileURL.getFile()));
        boolean fInDataSection = false;
        HashSet<String> sampleIDs = new HashSet<>();
        while (scanner.hasNextLine()) {
            String sLine = scanner.nextLine();
            List<String> values = Helper.split(sLine, ",");
            if (values.containsAll(dataHeaderList))
                fInDataSection = true;
            else if (fInDataSection)
                sampleIDs.add(values.get(subjectColIndex));
        }
        scanner.close();
        attemptPreloadingIndividuals(sampleIDs, progress);
    }

    public static String reverseComplement(String seq) {
        StringBuilder sb = new StringBuilder(seq.length());
        for (int i = seq.length() - 1; i >= 0; i--) {
            char c = Character.toUpperCase(seq.charAt(i));
            switch (c) {
                case 'A': sb.append('T'); break;
                case 'T': sb.append('A'); break;
                case 'C': sb.append('G'); break;
                case 'G': sb.append('C'); break;
                default: sb.append('N');
            }
        }
        return sb.toString();
    }
}