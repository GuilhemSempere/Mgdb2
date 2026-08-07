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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.dao.OptimisticLockingFailureException;

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
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataV3Id;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypeCodeManager;

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
    @Override
    protected void cleanupBeforeImport(MongoTemplate mongoTemplate, String sModule, GenotypingProject project, int importMode, String sRun) throws Exception {
        if (importMode == 2)
            mongoTemplate.getDb().drop();
        else if (project != null)
        {
            boolean fAnythingChanged = false;
            if (importMode == 1 || (project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
                fAnythingChanged = MgdbDao.removeProjectAndRelatedRecords(sModule, project.getId());
            else
                fAnythingChanged = MgdbDao.removeRunAndRelatedRecords(sModule, project.getId(), sRun, false);

            if (fAnythingChanged)
                MongoTemplateManager.updateDatabaseLastModification(sModule);

            if (Helper.estimDocCount(mongoTemplate, VariantRunData.class) == 0 && Helper.estimDocCount(mongoTemplate, VariantRunData.class) == 0 && m_fAllowDbDropIfNoGenotypingData && doesDatabaseSupportImportingUnknownVariants(sModule))
                mongoTemplate.getDb().drop();
        }
    }
//	/**
//	 * Import to mongo.
//	 *
//	 * @param sModule the module
//	 * @param sProject the project
//	 * @param sRun the run
//	 * @param sTechnology the technology
//     * @param nPloidy the ploidy level
//	 * @param mainFileUrl the main file URL
//     * @param assemblyName the assembly name
//	 * @param sampleToIndividualMap the sample-individual mapping
//     * @param fSkipMonomorphic whether or not to skip import of variants that have no polymorphism (where all individuals have the same genotype)
//	 * @param importMode the import mode
//	 * @return a project ID if it was created by this method, otherwise null
//	 * @throws Exception the exception
//	 */
//	public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, URL mainFileUrl, String assemblyName, HashMap<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode) throws Exception

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
        AtomicInteger totalParsedVariantCount = new AtomicInteger(0);
        AtomicInteger totalWrittenVariantCount = new AtomicInteger(0);
        final ArrayList<String> sampleIds = new ArrayList<>();
        progress.addStep("Processing variant lines");
        progress.moveToNextStep();

        int nNConcurrentThreads = Math.max(1, nNumProc);
        int nImportThreads = Math.max(1, (nNConcurrentThreads - 1) / 1);
        LOG.debug("Importing project '" + params.getProject() + "' into " + params.getModule() + " using " + nImportThreads + " threads");

        // --- DISPATCHER + QUEUE IMPLEMENTATION ---

        @SuppressWarnings("unchecked")
        BlockingQueue<VariantTask>[] workerQueues = new BlockingQueue[nImportThreads];
        for (int i = 0; i < nImportThreads; i++) {
            workerQueues[i] = new LinkedBlockingQueue<>();
        }

        // SHARED CACHE across all workers
        ConcurrentHashMap<String, VariantData> sharedVariantCache = new ConcurrentHashMap<>();

        final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
        if (assemblyIDs.isEmpty())
            assemblyIDs.add(null);

        final GenotypingProject finalProject = project;
        final MongoTemplate finalMongoTemplate = mongoTemplate;
        final Assembly finalAssembly = assembly;
        m_providedIdToSampleMap = new HashMap<String, GenotypingSample>();
        m_providedIdToCallsetMap = new HashMap<String, Callset>();
        final int runIndex = project.getRuns().contains(sRun) ? project.getRuns().indexOf(sRun) : project.getRuns().size();

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
                            totalParsedVariantCount,
                            totalWrittenVariantCount,
                            existingVariantIDs,
                            params.isSkipMonomorphic(),
                            sampleIds,
                            sampleToIndividualMap,
                            sharedVariantCache,
                            project.getId(),
                            runIndex
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
            Iterator<RawHapMapFeature> it = reader.iterator();
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
                    Type variantType = determineType(Arrays.stream(hmFeature.getAlleles()).map(allele -> Allele.create(allele)).collect(Collectors.toList()));

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
                            variantId = generatedIdBaseString + String.format("%09x", totalParsedVariantCount.getAndIncrement());
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

            for (BlockingQueue<VariantTask> queue : workerQueues)
                queue.put(VariantTask.POISON_PILL);

        } catch (Exception e) {
            progress.setError("Dispatcher failed: " + e.getMessage());
            LOG.error(progress.getError(), e);
        }

        // Wait for workers to finish
        for (int i = 0; i < nImportThreads; i++) {
            importThreads[i].join();
        }

        reader.close();

        if (progress.getError() != null || progress.isAborted())
            return 0;

        return totalWrittenVariantCount.get();
    }

    /**
     * Worker method that processes variant tasks
     * Uses shared cache across all workers to prevent duplicate variant creation
     */
    private void processVariantTasks(
            BlockingQueue<VariantTask> queue,
            MongoTemplate mongoTemplate,
            Integer nAssemblyId,
            GenotypingProject project,
            String sRun,
            Collection<Integer> assemblyIDs,
            ProgressIndicator progress,
            AtomicInteger totalParsedVariantCount,
            AtomicInteger totalWrittenVariantCount,
            HashMap<String, String> existingVariantIDs,
            boolean fSkipMonomorphic,
            ArrayList<String> sampleIds,
            Map<String, String> sampleToIndividualMap,
            ConcurrentHashMap<String, VariantData> sharedVariantCache,
            int projectID,
            int runIndex) throws Exception {

        HashSet<VariantData> unsavedVariants = new HashSet<>();
        HashSet<VariantRunData> unsavedRuns = new HashSet<>();

        final int chunkSize = Math.max(1, Math.min(1000, (int) Math.ceil((float) nMaxChunkSize / Math.max(1, sampleIds.size()))));
        int workerProcessed = 0;

        while (true) {
            VariantTask task = queue.take();
            if (task == VariantTask.POISON_PILL || progress.getError() != null || progress.isAborted())
                break;

            String variantId = task.variantId;

            // USE SHARED CACHE
            VariantData variant = sharedVariantCache.get(variantId);
            if (variant == null) {
                variant = mongoTemplate.findById(variantId, VariantData.class);
                if (variant == null) {
                    String id = ObjectId.isValid(variantId) ? "_" + variantId : variantId;
                    variant = new VariantData(id);
                }
                VariantData existing = sharedVariantCache.putIfAbsent(variantId, variant);
                if (existing != null) {
                    variant = existing;
                }
            }

            variant.getRuns().add(new Run(project.getId(), sRun));

            VariantRunData runToSave = addHapMapDataToVariant(
                    mongoTemplate,
                    variant,
                    nAssemblyId,
                    task.hmFeature,
                    project,
                    sRun,
                    sampleIds
                );

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

            workerProcessed++;

            if (workerProcessed % chunkSize == 0 && !unsavedVariants.isEmpty()) {
                VcfImport.persistVariantsAndGenotypesV3(!existingVariantIDs.isEmpty(), mongoTemplate,
                    unsavedVariants, unsavedRuns, projectID, runIndex);
                progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size()));

                unsavedVariants = new HashSet<>();
                unsavedRuns = new HashSet<>();
            }
        }

        // Save remaining
        if (!unsavedVariants.isEmpty()) {
            VcfImport.persistVariantsAndGenotypesV3(!existingVariantIDs.isEmpty(), mongoTemplate,
                unsavedVariants, unsavedRuns, projectID, runIndex);
            progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size()));
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

        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream()
                .collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));

        VariantRunData vrd = new VariantRunData(variantToFeed.getId());
		HashSet<Integer> ploidiesFound = new HashSet<>();

        List<List<List<Integer>>> genotypeArray = null;
        genotypeArray = new ArrayList<>();
        genotypeArray.add(new ArrayList<>());
        genotypeArray.get(0).add(new ArrayList<>());

		for (int i=0; i<hmFeature.getGenotypes().length; i++) {
            String genotype = hmFeature.getGenotypes()[i].toUpperCase();
            if (genotype.startsWith("N")) {
                genotypeArray.get(0).get(0).add(null);
                continue;    // we don't add missing genotypes
            }
            if (genotype.length() == 1) {
                String gtForIupacCode = iupacCodeConversionMap.get(genotype);
                if (gtForIupacCode != null)
                    genotype = gtForIupacCode;    // it's a IUPAC code, let's convert it to a pair of bases
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
                int numericCode = GenotypeCodeManager.createGenotypeEncoding(alleles, alleleIndexMap, mongoTemplate, new HashMap<>()); // FIXME: Add genotype code cache map


                genotypeArray.get(0).get(0).add(numericCode);

            } catch (NullPointerException npe) {
                throw new Exception("Some genotypes for variant "
                        + hmFeature.getContig() + ":" + hmFeature.getStart()
                        + " refer to alleles not declared at the beginning of the line!");
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
        vrd.setGenotypeArray(genotypeArray);
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