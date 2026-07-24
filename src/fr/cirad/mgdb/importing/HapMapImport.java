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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.*;
import fr.cirad.tools.ProgressIndicator;
import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapCodec;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapFeature;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class HapMapImport extends AbstractGenotypeImport<FileImportParameters> {

    private static final Logger LOG = Logger.getLogger(VariantData.class);

    private int nNumProc = Runtime.getRuntime().availableProcessors();

    private static HashMap<String, String> iupacCodeConversionMap = new HashMap<>();

    static {
        iupacCodeConversionMap.put("U", "TT");
        iupacCodeConversionMap.put("R", "AG");
        iupacCodeConversionMap.put("Y", "CT");
        iupacCodeConversionMap.put("S", "GC");
        iupacCodeConversionMap.put("W", "AT");
        iupacCodeConversionMap.put("K", "GT");
        iupacCodeConversionMap.put("M", "AC");
        iupacCodeConversionMap.put("N", "NN");
    }

    private FeatureReader<RawHapMapFeature> reader;
    
    public HapMapImport() {}

    public HapMapImport(String processID) {
        m_processID = processID;
    }

    public HapMapImport(boolean fCloseContextAfterImport) {
        this();
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    public HapMapImport(boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this();
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    public HapMapImport(String processID, boolean fCloseContextAfterImport) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    public HapMapImport(String processID, boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, HapMap file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

        File mainFile = new File(args[4]);
        if (!mainFile.exists() || mainFile.length() == 0)
            throw new Exception("File " + args[4] + " is missing or empty!");

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
                mode, //importMode,
                new File(args[4]).toURI().toURL()
        );
        new HapMapImport().importToMongo(params);
    }

    /**
     * Task class for dispatching variant processing
     */
    private static class VariantTask {
        public static final VariantTask POISON_PILL = new VariantTask(null, null);
        
        final RawHapMapFeature hmFeature;
        final String variantId;
        
        VariantTask(RawHapMapFeature hmFeature, String variantId) {
            this.hmFeature = hmFeature;
            this.variantId = variantId;
        }
    }

    @Override
    public long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sRun = params.getRun();
        Integer nPloidy = params.getPloidy();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();

        if (project == null || params.getImportMode() > 0) {
            if (params.getPloidy() != null) {
                project.setPloidyLevel(nPloidy);
            } else {
                progress.addStep("Attempting to guess ploidy level");
                progress.moveToNextStep();

                int nTestedVariantCount = 0;
                Iterator<RawHapMapFeature> it = reader.iterator();
                RawHapMapFeature hmFeature;
                while (nTestedVariantCount < 1000 && it.hasNext()) {
                    hmFeature = it.next();
                    if (hmFeature.getAlleles().length > 1) {
                        project.setPloidyLevel(hmFeature.getAlleles().length);
                        LOG.info("Guessed ploidy level for dataset to import: " + project.getPloidyLevel());
                        break;
                    }
                    nTestedVariantCount++;
                }
                if (project.getPloidyLevel() == 0)
                    LOG.warn("Unable to guess ploidy level for dataset to import: " + project.getPloidyLevel());
            }
        }

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, params.getAssemblyName());
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

        String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
        AtomicInteger totalProcessedVariantCount = new AtomicInteger(0);
        final ArrayList<String> sampleIds = new ArrayList<>();
        progress.addStep("Processing variant lines");
        progress.moveToNextStep();

        int nNConcurrentThreads = Math.max(1, nNumProc);
        LOG.debug("Importing project '" + params.getProject() + "' into " + params.getModule() + " using " + nNConcurrentThreads + " threads");

        Iterator<RawHapMapFeature> it = reader.iterator();
        
        // --- DISPATCHER + QUEUE IMPLEMENTATION ---
        
        int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
        @SuppressWarnings("unchecked")
        BlockingQueue<VariantTask>[] workerQueues = new BlockingQueue[nImportThreads];
        for (int i = 0; i < nImportThreads; i++) {
            workerQueues[i] = new LinkedBlockingQueue<>();
        }

        BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
        ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
        final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
        if (assemblyIDs.isEmpty())
            assemblyIDs.add(null);

        final GenotypingProject finalProject = project;
        final MongoTemplate finalMongoTemplate = mongoTemplate;
        final Assembly finalAssembly = assembly;
        m_providedIdToSampleMap = new HashMap<String, GenotypingSample>();
        m_providedIdToCallsetMap = new HashMap<String, Callset>();

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
                            totalProcessedVariantCount,
                            existingVariantIDs,
                            params.isSkipMonomorphic(),
                            sampleIds,
                            sampleToIndividualMap
                        );
                    } catch (Throwable t) {
                        progress.setError("Worker " + workerIndex + " failed: " + t.getMessage());
                        LOG.error(progress.getError(), t);
                    }
                }
            };
            importThreads[threadIndex].start();
        }

        // --- DISPATCHER RUNS IN MAIN THREAD ---
        try {
            boolean samplesInitialized = false;
            
            while (it.hasNext() && progress.getError() == null && !progress.isAborted()) {
                RawHapMapFeature hmFeature = it.next();
                
                if (!samplesInitialized) {
                    synchronized (sampleIds) {
                        if (sampleIds.isEmpty()) {
                            sampleIds.addAll(Arrays.asList(hmFeature.getSampleIDs()));
                            createCallSetsSamplesIndividuals(sampleIds, finalMongoTemplate, finalProject.getId(), sRun, sampleToIndividualMap, progress);
                            setSamplesPersisted(true);
                        }
                    }
                    samplesInitialized = true;
                }
                
                try {
                    Type variantType = determineType(Arrays.stream(hmFeature.getAlleles())
                        .map(allele -> Allele.create(allele))
                        .collect(Collectors.toList()));
                    
                    String sFeatureName = hmFeature.getName().trim();
                    
                    // --- SIMPLE VARIANT RESOLUTION ---
                    String variantId = null;
                    boolean hasValidId = !sFeatureName.isEmpty() && !".".equals(sFeatureName);
                    List<String> idAndSynonyms = hasValidId ? Arrays.asList(new String[]{sFeatureName}) : null;
                    
                    try {
                        for (String variantDescForPos : getIdentificationStrings(
                                variantType.toString(), 
                                hmFeature.getChr(), 
                                (long) hmFeature.getStart(), 
                                idAndSynonyms)) {
                            variantId = existingVariantIDs.get(variantDescForPos);
                            if (variantId != null) break;
                        }
                    } catch (Exception e) {
                        LOG.debug("Cannot build identification strings: " + e.getMessage());
                    }
                    
                    if (variantId == null) {
                        if (hasValidId) {
                            variantId = (ObjectId.isValid(sFeatureName) ? "_" : "") + sFeatureName;
                        } else {
                            variantId = generatedIdBaseString + String.format("%09x", totalProcessedVariantCount.getAndIncrement());
                        }
                    }
                    
                    // Check if monomorphic and should skip (only for new variants)
                    if (params.isSkipMonomorphic() && !existingVariantIDs.containsKey(variantId)) {
                        String[] distinctGTs = Arrays.stream(hmFeature.getGenotypes())
                            .filter(gt -> !"NA".equals(gt) && !"NN".equals(gt))
                            .distinct()
                            .toArray(String[]::new);
                        if (distinctGTs.length == 0 || 
                            (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split(variantType.equals(Type.SNP) ? "" : "/")).distinct().count() < 2)) {
                            continue;
                        }
                    }
                    
                    int workerIndex = Math.floorMod(variantId.hashCode(), nImportThreads);
                    VariantTask task = new VariantTask(hmFeature, variantId);
                    workerQueues[workerIndex].put(task);
                    
                } catch (Exception e) {
                    LOG.error("Error processing variant: " + e.getMessage(), e);
                    progress.setError("Error processing variant: " + e.getMessage());
                    return 0;
                }
            }
            
            for (BlockingQueue<VariantTask> queue : workerQueues) {
                queue.put(VariantTask.POISON_PILL);
            }
            
        } catch (Exception e) {
            progress.setError("Dispatcher failed: " + e.getMessage());
            LOG.error(progress.getError(), e);
        }

        // Wait for workers to finish
        for (int i = 0; i < nImportThreads; i++) {
            importThreads[i].join();
        }

        reader.close();
        saveService.shutdown();
        saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

        if (progress.getError() != null || progress.isAborted())
            return 0;

        return totalProcessedVariantCount.get();
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
            AtomicInteger totalProcessedVariantCount,
            HashMap<String, String> existingVariantIDs,
            boolean fSkipMonomorphic,
            ArrayList<String> sampleIds,
            Map<String, String> sampleToIndividualMap) throws Exception {
        
        HashSet<VariantData> unsavedVariants = new HashSet<>();
        HashSet<VariantRunData> unsavedRuns = new HashSet<>();
        HashMap<String, VariantData> variantCache = new HashMap<>();
        
        int chunkSize = Math.max(1, nMaxChunkSize / Math.max(1, sampleIds.size()));
        long processedVariants = 0;
        int localChunkSize = chunkSize;
        
        while (true) {
            VariantTask task = queue.take();
            if (task == VariantTask.POISON_PILL) {
                break;
            }
            
            String variantId = task.variantId;
            
            // Get or load variant from cache - NO POSITION VERIFICATION
            VariantData variant = variantCache.get(variantId);
            if (variant == null) {
                variant = mongoTemplate.findById(variantId, VariantData.class);
                if (variant == null) {
                    String id = ObjectId.isValid(variantId) ? "_" + variantId : variantId;
                    variant = new VariantData(id);
                }
                variantCache.put(variantId, variant);
            }
            
            // Add run to variant
            variant.getRuns().add(new Run(project.getId(), sRun));
            
            // Process the variant
            VariantRunData runToSave = addHapMapDataToVariant(
                mongoTemplate,
                variant,
                nAssemblyId,
                task.hmFeature,
                project,
                sRun,
                sampleIds
            );
            
            // Track the variant
            if (variant.getKnownAlleles().size() > 0) {
                if (!unsavedVariants.contains(variant)) {
                    unsavedVariants.add(variant);
                }
                if (!unsavedRuns.contains(runToSave)) {
                    unsavedRuns.add(runToSave);
                }
                
                for (Integer asmId : assemblyIDs) {
                    ReferencePosition rp = variant.getReferencePosition(asmId);
                    project.getContigs(asmId).add(rp == null ? "" : rp.getSequence());
                }
                project.getVariantTypes().add(variant.getType());
                project.getAlleleCounts().add(variant.getKnownAlleles().size());
            }
            
            int newCount = totalProcessedVariantCount.incrementAndGet();
            processedVariants++;
            
            if (processedVariants % localChunkSize == 0) {
                saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, saveService);
                
                variantCache.clear();
                unsavedVariants = new HashSet<>();
                unsavedRuns = new HashSet<>();
                
                progress.setCurrentStepProgress(newCount);
            }
            
            if (processedVariants % (localChunkSize * 50) == 0) {
                LOG.debug(newCount + " lines processed by worker");
            }
        }
        
        if (!unsavedVariants.isEmpty()) {
            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
        }
    }

    @Override
    protected void initReader(FileImportParameters params) throws IOException {
        reader = AbstractFeatureReader.getFeatureReader(params.getMainFileUrl().toString(), new RawHapMapCodec(), false);
    }

    @Override
    protected void closeResource() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected Integer findPloidyLevel(MongoTemplate mongoTemplate, Integer nPloidyParam, ProgressIndicator progress) throws IOException {
        Integer nPloidyLevel = null;
        if (nPloidyParam != null) {
            nPloidyLevel = nPloidyParam;
        } else {
            progress.addStep("Attempting to guess ploidy level");
            progress.moveToNextStep();
            int nTestedVariantCount = 0;
            Iterator<RawHapMapFeature> it = reader.iterator();
            RawHapMapFeature hmFeature;
            while (nTestedVariantCount < 1000 && it.hasNext()) {
                hmFeature = it.next();
                if (hmFeature.getAlleles().length > 1) {
                    nPloidyLevel = hmFeature.getAlleles().length;
                    LOG.info("Guessed ploidy level for dataset to import: " + nPloidyLevel);
                    break;
                }
                nTestedVariantCount++;
            }
            if (nPloidyLevel == 0)
                LOG.warn("Unable to guess ploidy level for dataset to import");
        }
        return nPloidyLevel;
    }

    private VariantRunData addHapMapDataToVariant(
            MongoTemplate mongoTemplate,
            VariantData variantToFeed,
            Integer nAssemblyId,
            RawHapMapFeature hmFeature,
            GenotypingProject project,
            String runName,
            List<String> individuals) throws Exception {
        
        // Determine variant type from alleles
        Type variantType = determineType(Arrays.stream(hmFeature.getAlleles())
            .map(allele -> Allele.create(allele))
            .collect(Collectors.toList()));
        
        int initialAlleleCount = variantToFeed.getKnownAlleles().size();
        boolean fNotSNP = !variantType.equals(Type.SNP) && !variantType.equals(Type.NO_VARIATION);

        if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType()))
            variantToFeed.setType(variantType.toString());
        else if (null != variantType && Type.NO_VARIATION != variantType && !variantToFeed.getType().equals(variantType.toString()))
            throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

        if (!"0".equals(hmFeature.getChr()) && hmFeature.getStart() > 0)
            if (variantToFeed.getReferencePosition(nAssemblyId) == null)
                variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(hmFeature.getChr(), hmFeature.getStart(), (long) hmFeature.getEnd()));

        if (variantToFeed.getKnownAlleles().size() == 0)
            variantToFeed.setKnownAlleles(Arrays.stream(hmFeature.getAlleles()).collect(Collectors.toList()));

        // Build allele index map
        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream()
            .collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));

        VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
        HashSet<Integer> ploidiesFound = new HashSet<>();
        
        for (int i = 0; i < hmFeature.getGenotypes().length; i++) {
            String genotype = hmFeature.getGenotypes()[i].toUpperCase();
            if (genotype.startsWith("N"))
                continue;

            if (genotype.length() == 1) {
                String gtForIupacCode = iupacCodeConversionMap.get(genotype);
                if (gtForIupacCode != null)
                    genotype = gtForIupacCode;
            }

            List<String> alleles = null;
            if (genotype.contains("/")) {
                alleles = Helper.split(genotype, "/");
                ploidiesFound.add(alleles.size());
            } else if (alleleIndexMap.containsKey(genotype)) {
                alleles = Collections.nCopies(project.getPloidyLevel(), genotype);
            } else if (!fNotSNP) {
                alleles = Arrays.asList(genotype.split(""));
            }

            String sIndOrSpId = individuals.get(i);
            if (alleles == null || alleles.isEmpty()) {
                LOG.warn("Ignoring invalid genotype \"" + genotype + "\" for variant " + variantToFeed.getId() + " and individual " + sIndOrSpId);
                continue;
            }

            try {
                SampleGenotype aGT = new SampleGenotype(alleles.stream()
                    .map(allele -> alleleIndexMap.get(allele))
                    .sorted()
                    .map(index -> index.toString())
                    .collect(Collectors.joining("/")));
                vrd.getSampleGenotypes().put(m_providedIdToCallsetMap.get(sIndOrSpId).getId(), aGT);
            } catch (NullPointerException npe) {
                throw new Exception("Some genotypes for variant " + hmFeature.getContig() + ":" + hmFeature.getStart() + " refer to alleles not declared at the beginning of the line!");
            }
        }

        if (ploidiesFound.size() > 1)
            throw new Exception("Ambiguous ploidy level, please explicitly specify correct ploidy");

        if (project.getPloidyLevel() == 0 && !ploidiesFound.isEmpty())
            project.setPloidyLevel(ploidiesFound.iterator().next());

        project.getVariantTypes().add(variantType.toString());

        if (project.getId() > 1 || project.getRuns().size() > 0)
            updateExistingVrdAlleles(mongoTemplate, initialAlleleCount, variantToFeed);

        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setPositions(variantToFeed.getPositions());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        return vrd;
    }

    @Override
    protected GenotypingProject createProject(MongoTemplate mongoTemplate, String sProject, String sTechnology, Integer nPloidy, ProgressIndicator progress) throws IOException {
        GenotypingProject project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
        project.setName(sProject);
        project.setTechnology(sTechnology);
        if (nPloidy != null)
            project.setPloidyLevel(nPloidy);
        else {
            progress.addStep("Attempting to guess ploidy level");
            progress.moveToNextStep();

            int nTestedVariantCount = 0;
            Iterator<RawHapMapFeature> it = reader.iterator();
            RawHapMapFeature hmFeature;
            while (nTestedVariantCount < 1000 && it.hasNext()) {
                hmFeature = it.next();
                if (hmFeature.getAlleles().length > 1) {
                    project.setPloidyLevel(hmFeature.getAlleles().length);
                    LOG.info("Guessed ploidy level for dataset to import: " + project.getPloidyLevel());
                    break;
                }
                nTestedVariantCount++;
            }
            if (project.getPloidyLevel() == 0)
                LOG.warn("Unable to guess ploidy level for dataset to import: " + project.getPloidyLevel());
        }
        return project;
    }
}