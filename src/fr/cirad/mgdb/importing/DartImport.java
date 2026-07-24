package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

public class DartImport extends AbstractGenotypeImport<FileImportParameters> {

    private static final Logger LOG = Logger.getLogger(VariantData.class);

    private DartIterator dartIterator;
    private int nNumProc = Runtime.getRuntime().availableProcessors();

    public DartImport(){}

    public DartImport(String processID) {
        m_processID = processID;
    }

    static final class DartIterator implements Iterator<List<DartInfo>> {
        Boolean twoRow = null;
        Scanner scanner;
        HashMap<String, Integer> fieldPositions;
        String[] columnNames;
        
        public DartIterator(Scanner scanner, String[] columnNames, HashMap<String, Integer> fieldPositions) {
            this.scanner = scanner;
            this.columnNames = columnNames;
            this.fieldPositions = fieldPositions;
        }

        @Override
        public boolean hasNext() {
            return scanner.hasNextLine();
        }

        @Override
        public List<DartInfo> next() {
            String line = scanner.nextLine();
            if (twoRow == null) {
                String alleleID1 = line.split(",")[0].split("[^0-9]")[0];
                String line1 = line;
                line = scanner.nextLine();
                String alleleID2 = line.split(",")[0].split("[^0-9]")[0];
                twoRow = alleleID1.equals(alleleID2);
                return genericDartLine(line1, line, scanner, fieldPositions, twoRow, columnNames);
            } else {
                return genericDartLine(line, null, scanner, fieldPositions, twoRow, columnNames);
            }
        }
        
        public void close() {
            scanner.close();
        }
    }

    public DartIterator getDartInfo(URL url) throws Exception {
        Scanner scanner = new Scanner(new BufferedReader(new InputStreamReader(url.openStream())));
        HashMap<String, Integer> fieldPositions = new HashMap<String, Integer>();
        String[] columnNames = null;

        String line = scanner.nextLine();

        while (scanner.hasNextLine() && line.startsWith("*,"))
            line = scanner.nextLine();

        if (line == null)
            throw new Exception("This file is not in a valid Dart format");

        columnNames = line.split(",");
        colLoop: for (int i = 0; i < columnNames.length; i++) {
            for (String mandatoryColPrefix : new String[] {"Chrom_", "ChromPos_"})
                if (columnNames[i].startsWith(mandatoryColPrefix)) {
                    fieldPositions.put(mandatoryColPrefix + "*", i);
                    continue colLoop;
                }
            fieldPositions.put(columnNames[i], i);
        }
        for (String mandatoryCol : new String[] {"AlleleID", "AlleleSequence", "TrimmedSequence", "SnpPosition", "CallRate", "FreqHomRef", "FreqHomSnp", "FreqHets", "RepAvg", "Chrom_*", "ChromPos_*"})
            if (fieldPositions.get(mandatoryCol) == null)
                throw new Exception("Unable to find mandatory field '" + mandatoryCol + "' in Dart file header!");

        DartIterator dartIterator = new DartIterator(scanner, columnNames, fieldPositions);
        return dartIterator;
    }

    private static List<DartInfo> genericDartLine(String line, String startLine, Scanner scanner, HashMap<String, Integer> fieldPositions, boolean tworow, String[] columnNames) {
        List<DartInfo> result = new ArrayList<>();
        String[] columns = line.split(",");
        
        if (columns.length != columnNames.length)
            throw new Error("Line has " + columns.length + " fields instead of " + columnNames.length + ": " + line.split(",")[0]);
        
        String alleleID = columns[fieldPositions.get("AlleleID")];
        DartInfo dart = new DartInfo(alleleID);
        dart.setAlleleSequence(columns[fieldPositions.get("AlleleSequence")]);
        dart.setTrimmedSequence(columns[fieldPositions.get("TrimmedSequence")]);
        dart.setChrom(columns[fieldPositions.get("Chrom_*")]);
        dart.setChromPos(Integer.parseInt(columns[fieldPositions.get("ChromPos_*")]));
        dart.setSnpPos(Integer.parseInt(columns[fieldPositions.get("SnpPosition")]));
        dart.setCallRate(Float.parseFloat(columns[fieldPositions.get("CallRate")]));
        dart.setFreqHomRef(Float.parseFloat(columns[fieldPositions.get("FreqHomRef")]));
        dart.setFreqHomSnp(Float.parseFloat(columns[fieldPositions.get("FreqHomSnp")]));
        dart.setFreqHets(Float.parseFloat(columns[fieldPositions.get("FreqHets")]));
        int sampleIndex = fieldPositions.get("RepAvg") + 1;
        String[] samplesName = Arrays.copyOfRange(columnNames, sampleIndex, columnNames.length);
        dart.setSampleIDs(samplesName);
        int numberSamples = fieldPositions.size() - sampleIndex;
        String[] genotypes = new String[numberSamples];
        String[] samples = Arrays.copyOfRange(columns, sampleIndex, columns.length);
        int altIndex = dart.getAlleleID().indexOf('>');
        char ref = dart.getAlleleID().charAt(altIndex - 1);
        char alt = dart.getAlleleID().charAt(altIndex + 1);
        dart.setAlleles(new String[]{"" + ref, "" + alt});
        if (!tworow) {
            for (int i = 0; i < numberSamples; i++) {
                if (samples[i].equals("-"))
                    genotypes[i] = genotypeOfSample(ref, alt, 3);
                else
                    genotypes[i] = genotypeOfSample(ref, alt, Integer.parseInt(samples[i]));
            }
            dart.setGenotypes(genotypes);
            if (startLine != null){
                result.add(dart);
                result.addAll(genericDartLine(startLine, null, scanner, fieldPositions, tworow, columnNames));
                return result;
            }
        } else {
            if (startLine != null)
                line = startLine;
            else
                line = scanner.nextLine();
            String[] columns2 = line.split(",");
            String[] samples2 = Arrays.copyOfRange(columns2, sampleIndex, columns2.length);
            for (int i = 0; i < numberSamples; i++) {
                if (samples[i].equals("-") || samples2[i].equals("-"))
                    genotypes[i] = genotypeOfSampleTwoRow(ref, alt, 3, 3);
                else
                    genotypes[i] = genotypeOfSampleTwoRow(ref, alt, Integer.parseInt(samples[i]), Integer.parseInt(samples2[i]));
            }
            dart.setGenotypes(genotypes);
        }

        result.add(dart);
        return result;
    }

    public static String genotypeOfSample(char ref, char alt, int state) {
        switch (state) {
            case 0: return "" + ref + ref;
            case 1: return "" + alt + alt;
            case 2: return "" + ref + alt;
            case 3: return "NN";
            default: throw new Error("Sample's state have to be 0, 1 or 2 on OneRow");
        }
    }

    public static String genotypeOfSampleTwoRow(char ref, char alt, int state1, int state2) {
        switch (state1) {
            case 0:
                if (state2 == 0) return "" + ref + ref;
                if (state2 == 1) return "" + ref + alt;
                throw new Error("Sample's state have to be 0 or 1 on TwoRow");
            case 1:
                if (state2 == 0) return "" + alt + ref;
                if (state2 == 1) return "" + alt + alt;
                throw new Error("Sample's state have to be 0 or 1 on TwoRow");
            case 3: return "NN";
            default: throw new Error("Sample's state have to be 0 or 1 on TwoRow");
        }
    }

    /**
     * Task class for dispatching variant processing
     */
    private static class VariantTask {
        public static final VariantTask POISON_PILL = new VariantTask(null, null);
        
        final DartInfo dartFeature;
        final String variantId;
        
        VariantTask(DartInfo dartFeature, String variantId) {
            this.dartFeature = dartFeature;
            this.variantId = variantId;
        }
    }

    @Override
    protected long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sModule = params.getModule();
        String sProject = params.getProject();
        String sRun = params.getRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

        String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
        AtomicInteger totalProcessedVariantCount = new AtomicInteger(0);
        final ArrayList<String> sampleIds = new ArrayList<>();
        progress.addStep("Processing variant lines");
        progress.moveToNextStep();

        int nNConcurrentThreads = Math.max(1, nNumProc);
        LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nNConcurrentThreads + " threads");

        DartIterator dataReader = getDartInfo(params.getMainFileUrl());
        
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

        // Setup VCF header
        VCFInfoHeaderLine headerLineGT = new VCFInfoHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
        VCFInfoHeaderLine headerLineAS = new VCFInfoHeaderLine("AS", 2, VCFHeaderLineType.String, "AlleleSequence");
        VCFInfoHeaderLine headerLineSP = new VCFInfoHeaderLine("SP", 3, VCFHeaderLineType.Integer, "SnpPosition");
        VCFInfoHeaderLine headerLineCR = new VCFInfoHeaderLine("CR", 4, VCFHeaderLineType.Float, "CallRate");
        VCFInfoHeaderLine headerLineFHR = new VCFInfoHeaderLine("FHR", 5, VCFHeaderLineType.Float, "FreqHomRef");
        VCFInfoHeaderLine headerLineFHS = new VCFInfoHeaderLine("FHS", 6, VCFHeaderLineType.Float, "FreqHomSnp");
        VCFInfoHeaderLine headerLineFH = new VCFInfoHeaderLine("FH", 7, VCFHeaderLineType.Float, "FreqHet");

        VCFHeader header = new VCFHeader(new HashSet<>(Arrays.asList(headerLineGT, headerLineAS, headerLineSP, headerLineCR, headerLineFHR, headerLineFHS, headerLineFH)));
        finalMongoTemplate.save(new DBVCFHeader(new DBVCFHeader.VcfHeaderId(finalProject.getId(), sRun), header));

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
                            fSkipMonomorphic,
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
            
            while (dataReader.hasNext() && progress.getError() == null && !progress.isAborted()) {
                List<DartInfo> dartFeatures = dataReader.next();
                
                if (!samplesInitialized && !dartFeatures.isEmpty()) {
                    synchronized (sampleIds) {
                        if (sampleIds.isEmpty()) {
                            sampleIds.addAll(Arrays.asList(dartFeatures.iterator().next().getSampleIDs()));
                            createCallSetsSamplesIndividuals(sampleIds, finalMongoTemplate, finalProject.getId(), sRun, sampleToIndividualMap, progress);
                            setSamplesPersisted(true);
                        }
                    }
                    samplesInitialized = true;
                }
                
                for (DartInfo dartFeature : dartFeatures) {
                    try {
                        Type variantType = determineType(Arrays.stream(dartFeature.getAlleles())
                            .map(allele -> Allele.create(allele))
                            .collect(Collectors.toList()));
                        
                        String sFeatureName = dartFeature.getAlleleID().trim();
                        
                        // --- SIMPLE VARIANT RESOLUTION ---
                        String variantId = null;
                        boolean hasValidId = !sFeatureName.isEmpty() && !".".equals(sFeatureName);
                        List<String> idAndSynonyms = hasValidId ? Arrays.asList(new String[]{sFeatureName}) : null;
                        
                        try {
                            for (String variantDescForPos : getIdentificationStrings(
                                    variantType.toString(), 
                                    dartFeature.getChrom(), 
                                    (long) dartFeature.getStart(), 
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
                        if (fSkipMonomorphic && !existingVariantIDs.containsKey(variantId)) {
                            String[] distinctGTs = Arrays.stream(dartFeature.getGenotypes())
                                .filter(gt -> !"NA".equals(gt) && !"NN".equals(gt))
                                .distinct()
                                .toArray(String[]::new);
                            if (distinctGTs.length == 0 || 
                                (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split(variantType.equals(Type.SNP) ? "" : "/")).distinct().count() < 2)) {
                                continue;
                            }
                        }
                        
                        int workerIndex = Math.floorMod(variantId.hashCode(), nImportThreads);
                        VariantTask task = new VariantTask(dartFeature, variantId);
                        workerQueues[workerIndex].put(task);
                        
                    } catch (Exception e) {
                        LOG.error("Error processing variant: " + e.getMessage(), e);
                        progress.setError("Error processing variant: " + e.getMessage());
                        return 0;
                    }
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

        dataReader.close();
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
            
            // Get or load variant from cache
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
            VariantRunData runToSave = addDartSeqDataToVariant(
                mongoTemplate,
                variant,
                nAssemblyId,
                task.dartFeature,
                project,
                sRun,
                sampleIds,
                existingVariantIDs
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
    protected void initReader(FileImportParameters params) throws Exception {
        dartIterator = getDartInfo(params.getMainFileUrl());
    }

    @Override
    protected void closeResource() throws IOException {
        if (dartIterator != null)
            dartIterator.close();
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
            variantLoop:
            while (nTestedVariantCount < 1000 && dartIterator.hasNext()) {
                List<DartInfo> dartFeatures = dartIterator.next();
                for (DartInfo dartFeature : dartFeatures)
                    if (dartFeature.getAlleles().length > 1) {
                        nPloidyLevel = dartFeature.getAlleles().length;
                        LOG.info("Guessed ploidy level for dataset to import: " + nPloidyLevel);
                        break variantLoop;
                    }
                nTestedVariantCount++;
            }
            if (nPloidyLevel == null)
                LOG.warn("Unable to guess ploidy level for dataset to import");
        }
        return nPloidyLevel;
    }

    private VariantRunData addDartSeqDataToVariant(
            MongoTemplate mongoTemplate, 
            VariantData variantToFeed, 
            Integer nAssemblyId, 
            DartInfo dartFeature, 
            GenotypingProject project, 
            String runName, 
            List<String> individuals, 
            HashMap<String, String> existingVariantIDs) throws Exception {
        
        // Determine variant type from alleles
        Type variantType = determineType(Arrays.stream(dartFeature.getAlleles())
            .map(allele -> Allele.create(allele))
            .collect(Collectors.toList()));
        
        int initialAlleleCount = variantToFeed.getKnownAlleles().size();
        boolean fSNP = variantType.equals(Type.SNP);

        if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType()))
            variantToFeed.setType(variantType.toString());
        else if (null != variantType && Type.NO_VARIATION != variantType && !variantToFeed.getType().equals(variantType.toString()))
            throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

        if (variantToFeed.getReferencePosition(nAssemblyId) == null)
            variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(dartFeature.getChrom(), dartFeature.getStart(), (long) dartFeature.getEnd()));

        if (variantToFeed.getKnownAlleles().size() == 0)
            variantToFeed.setKnownAlleles(Arrays.stream(dartFeature.getAlleles()).collect(Collectors.toList()));

        VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
        
        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream()
            .collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));
        
        HashSet<Integer> ploidiesFound = new HashSet<>();
        String[] genotypes = dartFeature.getGenotypes();
        
        for (int i = 0; i < genotypes.length; i++) {
            String genotype = genotypes[i].toUpperCase();
            if (genotype.startsWith("N"))
                continue;

            List<String> alleles = null;
            if (genotype.contains("/")) {
                alleles = Helper.split(genotype, "/");
                ploidiesFound.add(alleles.size());
            } else if (alleleIndexMap.containsKey(genotype)) {
                alleles = Collections.nCopies(project.getPloidyLevel(), genotype);
            } else if (fSNP) {
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
                GenotypingSample sample = m_providedIdToSampleMap.get(sIndOrSpId);
                if (sample == null)
                    throw new Exception("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
                Callset callset = m_providedIdToCallsetMap.get(sIndOrSpId);
                vrd.getSampleGenotypes().put(callset.getId(), aGT);
            } catch (NullPointerException npe) {
                throw new Exception("Some genotypes for variant " + dartFeature.getChrom() + ":" + dartFeature.getStart() + " refer to alleles not declared at the beginning of the line!");
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
        
        vrd.getAdditionalInfo().put("AS", dartFeature.getAlleleSequence());
        vrd.getAdditionalInfo().put("SP", dartFeature.getSnpPos());
        vrd.getAdditionalInfo().put("CR", dartFeature.getCallRate());
        vrd.getAdditionalInfo().put("FHR", dartFeature.getFreqHomRef());
        vrd.getAdditionalInfo().put("FHS", dartFeature.getFreqHomSnp());
        vrd.getAdditionalInfo().put("FH", dartFeature.getFreqHets());
        
        return vrd;
    }
}