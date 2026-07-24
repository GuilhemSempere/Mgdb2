package fr.cirad.mgdb.importing.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.parameters.ImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.ProgressIndicator;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * Base class for genotyping data import procedures that need refactoring provided files in a marker-oriented manner
 * 
 * @author sempere
 */
public abstract class RefactoredImport<T extends ImportParameters> extends AbstractGenotypeImport<T> {

    private static final Logger LOG = Logger.getLogger(RefactoredImport.class);

    protected boolean m_fImportUnknownVariants = false;
    protected int m_ploidy = 2;
    protected Integer m_maxExpectedAlleleCount = null;    // if set, will issue warnings when exceeded
    final static protected String validAlleleRegex = "[\\*AaTtGgCcNnIiDd]+".intern();

    public void setMaxExpectedAlleleCount(Integer maxExpectedAlleleCount) {
        m_maxExpectedAlleleCount = maxExpectedAlleleCount;
    }

    public void setPloidy(int ploidy) {
        m_ploidy = ploidy;
    }

    /**
     * Task class for dispatching variant processing
     */
    private static class VariantTask {
        public static final VariantTask POISON_PILL = new VariantTask(null, null, null, null, null);
        
        final String line;
        final String providedVariantId;
        final String canonicalVariantId;
        final String sequence;
        final Long bpPosition;
        
        VariantTask(String line, String providedVariantId, String canonicalVariantId,
                    String sequence, Long bpPosition) {
            this.line = line;
            this.providedVariantId = providedVariantId;
            this.canonicalVariantId = canonicalVariantId;
            this.sequence = sequence;
            this.bpPosition = bpPosition;
        }
    }

    public long importTempFileContents(ProgressIndicator progress, int nNConcurrentThreads, MongoTemplate mongoTemplate, Integer nAssemblyId, File tempFile, LinkedHashMap<String, String> providedVariantPositions, HashMap<String, String> existingVariantIDs, GenotypingProject project, String sRun, HashMap<String, ArrayList<String>> inconsistencies, LinkedHashMap<String, String> orderedIndividualToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, HashSet<Integer> indexesOfLinesThatMustBeSkipped, boolean fSkipMonomorphic) throws Exception {
        String[] individuals = orderedIndividualToPopulationMap.keySet().toArray(new String[orderedIndividualToPopulationMap.size()]);
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger ignoredVariants = new AtomicInteger(0);

        try {
            String info = "Importing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            progress.setPercentageEnabled(true);

            final int nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / individuals.length);
            LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);

            LinkedHashSet<String> individualsWithoutPopulation = new LinkedHashSet<>();
            for (String sIndOrSpId : orderedIndividualToPopulationMap.keySet()) {
                GenotypingSample sample = m_providedIdToSampleMap.get(sIndOrSpId);
                if (sample == null) {
                    progress.setError("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
                    return 0;
                }

                String sIndividual = sample.getIndividual();
                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                boolean fAlreadyExists = ind != null;
                boolean fNeedToSave = true;
                if (!fAlreadyExists)
                    ind = new Individual(sIndividual);
                String sPop = orderedIndividualToPopulationMap.get(sIndOrSpId);
                if (sPop != null)
                    ind.setPopulation(sPop);
                else {
                    String firstEncounteredDigit = sIndividual.chars().filter(Character::isDigit).mapToObj(Character::toString).findFirst().orElse(null);
                    Integer firstDigitPos = firstEncounteredDigit == null ? null : sIndividual.indexOf(firstEncounteredDigit);
                    if (firstDigitPos != null && sIndividual.length() > firstDigitPos && !sIndividual.substring(0, firstDigitPos).matches(".*\\d+.*") && sIndividual.substring(firstDigitPos).matches("\\d+"))
                        ind.setPopulation(sIndividual.substring(0, firstDigitPos));
                    else {
                        individualsWithoutPopulation.add(sIndividual);
                        if (fAlreadyExists)
                            fNeedToSave = false;
                    }
                }

                if (fNeedToSave)
                    mongoTemplate.save(ind);
            }

            if (!individualsWithoutPopulation.isEmpty() && populationCodesExpected())
                LOG.warn("Unable to find population code for individuals: " + StringUtils.join(individualsWithoutPopulation, ", "));

            final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
            if (assemblyIDs.isEmpty())
                assemblyIDs.add(null);    // old-style, assembly-less DB

            final int projId = project.getId();

            // --- DISPATCHER + QUEUE IMPLEMENTATION ---
            
            // Create worker queues
            int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
            BlockingQueue<VariantTask>[] workerQueues = new BlockingQueue[nImportThreads];
            for (int i = 0; i < nImportThreads; i++) {
                workerQueues[i] = new LinkedBlockingQueue<>();
            }

            // Save service
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());

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
                                mongoTemplate,
                                nAssemblyId,
                                individuals,
                                orderedIndividualToPopulationMap,
                                nonSnpVariantTypeMap,
                                project,
                                sRun,
                                nNumberOfVariantsToSaveAtOnce,
                                assemblyIDs,
                                progress,
                                saveService,
                                count,
                                providedVariantPositions.size(),
                                inconsistencies,
                                m_maxExpectedAlleleCount,
                                ignoredVariants,
                                existingVariantIDs,
                                fSkipMonomorphic,
                                m_ploidy,
                                m_fImportUnknownVariants
                            );
                        } catch (Throwable t) {
                            progress.setError("Worker " + workerIndex + " failed with " + t.getClass().getSimpleName() + ": " + t.getMessage());
                            LOG.error(progress.getError(), t);
                        }
                    }
                };
                importThreads[threadIndex].start();
            }

            // Dispatcher thread
            Thread dispatcherThread = new Thread() {
                @Override
                public void run() {
                    try (BufferedReader reader = new BufferedReader(new FileReader(tempFile))) {
                        int lineIndex = -1;
                        String line;
                        while ((line = reader.readLine()) != null) {
                            lineIndex++;
                            
                            if (indexesOfLinesThatMustBeSkipped != null && 
                                indexesOfLinesThatMustBeSkipped.contains(lineIndex)) {
                                continue;
                            }
                            
                            String[] splitLine = line.split("\t");
                            String providedVariantId = splitLine[0];
                            
                            // Use shared resolveVariantInfo method from AbstractGenotypeImport
                            ResolvedVariantInfo resolvedInfo = resolveVariantInfo(
                                providedVariantId,
                                providedVariantPositions,
                                existingVariantIDs,
                                nonSnpVariantTypeMap,
                                m_fImportUnknownVariants
                            );
                            
                            if (resolvedInfo.canonicalVariantId == null && !m_fImportUnknownVariants) {
                                ignoredVariants.incrementAndGet();
                                continue;
                            }
                            
                            if (fSkipMonomorphic && resolvedInfo.canonicalVariantId == null) {
                                String[] distinctGTs = Arrays.stream(splitLine, 1, splitLine.length)
                                    .filter(gt -> !gt.isEmpty())
                                    .distinct()
                                    .toArray(String[]::new);
                                if (distinctGTs.length == 0 || 
                                    (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2)) {
                                    continue;
                                }
                            }
                            
                            String idToDispatch = resolvedInfo.canonicalVariantId != null ? 
                                resolvedInfo.canonicalVariantId : providedVariantId;
                            int workerIndex = Math.floorMod(idToDispatch.hashCode(), nImportThreads);
                            
                            VariantTask task = new VariantTask(
                                line,
                                providedVariantId,
                                resolvedInfo.canonicalVariantId,
                                resolvedInfo.sequence,
                                resolvedInfo.bpPosition
                            );
                            
                            workerQueues[workerIndex].put(task);
                        }
                        
                        for (BlockingQueue<VariantTask> queue : workerQueues) {
                            queue.put(VariantTask.POISON_PILL);
                        }
                        
                    } catch (Exception e) {
                        progress.setError("Dispatcher failed: " + e.getMessage());
                        LOG.error(progress.getError(), e);
                    }
                }
            };
            dispatcherThread.start();

            dispatcherThread.join();
            for (int i = 0; i < nImportThreads; i++) {
                importThreads[i].join();
            }
            
            saveService.shutdown();
            saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

            if (ignoredVariants.get() > 0)
                LOG.warn("Number of ignored variants: " + ignoredVariants);

            if (progress.getError() != null || progress.isAborted())
                return count.get();

            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);
        } finally {
            // cleanup
        }
        return count.get();
    }

    /**
     * Worker method that processes variant tasks with caching
     */
    private void processVariantTasks(BlockingQueue<VariantTask> queue,
            MongoTemplate mongoTemplate,
            Integer nAssemblyId,
            String[] individuals,
            LinkedHashMap<String, String> orderedIndividualToPopulationMap,
            Map<String, Type> nonSnpVariantTypeMap,
            GenotypingProject project,
            String sRun,
            int chunkSize,
            Collection<Integer> assemblyIDs,
            ProgressIndicator progress,
            ExecutorService saveService,
            AtomicInteger count,
            int totalVariants,
            HashMap<String, ArrayList<String>> inconsistencies,
            Integer maxExpectedAlleleCount,
            AtomicInteger ignoredVariants,
            HashMap<String, String> existingVariantIDs,
            boolean fSkipMonomorphic,
            int ploidy,
            boolean importUnknownVariants) throws Exception {
        
        HashSet<VariantData> unsavedVariants = new HashSet<>();
        HashSet<VariantRunData> unsavedRuns = new HashSet<>();
        HashMap<String, VariantData> variantCache = new HashMap<>(); // Per-worker cache
        
        long processedVariants = 0;
        
        while (true) {
            VariantTask task = queue.take();
            if (task == VariantTask.POISON_PILL) {
                break;
            }
            
            String variantId = task.canonicalVariantId != null ? 
                task.canonicalVariantId : task.providedVariantId;
            
            if (variantId != null && variantId.startsWith("*")) {
                LOG.warn("Skipping deprecated variant data: " + task.providedVariantId);
                continue;
            }
            
            VariantData variant = variantCache.get(variantId);
            if (variant == null) {
                variant = mongoTemplate.findById(variantId, VariantData.class);
                if (variant == null) {
                    String id = ObjectId.isValid(variantId) ? "_" + variantId : variantId;
                    variant = new VariantData(id);
                }
                variantCache.put(variantId, variant);
            }
            
            variant.getRuns().add(new Run(project.getId(), sRun));
            
            String[] splitLine = task.line.split("\t");
            String[][] alleles = new String[individuals.length][ploidy];
            
            int nIndividualIndex = 0;
            while (nIndividualIndex < individuals.length) {
                if (splitLine.length > nIndividualIndex + 1 && !"".equals(splitLine[nIndividualIndex + 1])) {
                    String[] genotype = splitLine[nIndividualIndex + 1].split("/");
                    boolean fInconsistentData = false;
                    if (inconsistencies != null && !inconsistencies.isEmpty()) {
                        ArrayList<String> inconsistentIndividuals = inconsistencies.get(variant.getId());
                        fInconsistentData = inconsistencies != null && !inconsistencies.isEmpty() && 
                            inconsistentIndividuals != null && inconsistentIndividuals.contains(individuals[nIndividualIndex]);
                    }
                    
                    if (fInconsistentData) {
                        LOG.warn("Not adding inconsistent data: " + task.providedVariantId + " / " + individuals[nIndividualIndex]);
                    } else {
                        if (!importUnknownVariants && maxExpectedAlleleCount != null && 
                            maxExpectedAlleleCount == 2 && variant.getKnownAlleles().size() == 2 && 
                            variant.getType().equals(Type.INDEL.toString()) && 
                            (Arrays.stream(genotype).filter(all -> "I".equalsIgnoreCase(all) || "D".equalsIgnoreCase(all))).count() > 0) {
                            
                            if (variant.getKnownAlleles().get(0).length() == variant.getKnownAlleles().get(1).length()) {
                                LOG.warn("Unable to recognize INDEL alleles for variant " + variant.getVariantId() + " because both have the same length!");
                            }
                            String shortAllele = variant.getKnownAlleles().get(0).length() > variant.getKnownAlleles().get(1).length() ? 
                                variant.getKnownAlleles().get(1) : variant.getKnownAlleles().get(0);
                            String longAllele = shortAllele.equals(variant.getKnownAlleles().get(0)) ? 
                                variant.getKnownAlleles().get(1) : variant.getKnownAlleles().get(0);
                            for (int i = 0; i < genotype.length; i++) {
                                genotype[i] = "I".equalsIgnoreCase(genotype[i]) ? longAllele : shortAllele;
                            }
                        }
                        alleles[nIndividualIndex] = genotype;
                    }
                }
                nIndividualIndex++;
            }
            
            VariantRunData runToSave = addDataToVariant(
                mongoTemplate,
                variant,
                nAssemblyId,
                task.sequence,
                task.bpPosition,
                orderedIndividualToPopulationMap,
                nonSnpVariantTypeMap,
                alleles,
                project,
                sRun,
                importUnknownVariants
            );
            
            if (maxExpectedAlleleCount != null && variant.getKnownAlleles().size() > maxExpectedAlleleCount) {
                LOG.warn("Variant " + variant.getId() + " (" + task.providedVariantId + ") has more than " + maxExpectedAlleleCount + " alleles!");
            }
            
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
            } else {
                ReferencePosition rp = nAssemblyId != null ? variant.getReferencePosition(nAssemblyId) : null;
                LOG.info("Skipping variant " + task.providedVariantId + 
                    (rp != null ? " positioned at " + rp.getSequence() + ":" + rp.getStartSite() : "") + 
                    " because its alleles are not known (only missing data provided so far)");
            }
            
            int newCount = count.incrementAndGet();
            processedVariants++;
            
            if (processedVariants % chunkSize == 0) {
                saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, saveService);
                
                variantCache.clear();
                unsavedVariants = new HashSet<>();
                unsavedRuns = new HashSet<>();
                
                progress.setCurrentStepProgress(newCount * 100 / totalVariants);
            }
            
            if (processedVariants % (chunkSize * 50) == 0) {
                LOG.debug(newCount + " lines processed by worker");
            }
        }
        
        if (!unsavedVariants.isEmpty()) {
            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
        }
    }

    protected boolean populationCodesExpected() {
        return false;
    }

    protected VariantRunData addDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId, String sequence, Long bpPos, LinkedHashMap<String, String> orderedIndOrSpToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, String[][] alleles, GenotypingProject project, String runName, boolean fImportUnknownVariants) throws Exception {
        VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), runName, variantToFeed.getId()));

        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream().collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));
        int i = -1, initialAlleleCount = variantToFeed.getKnownAlleles().size();
        for (String sIndOrSp : orderedIndOrSpToPopulationMap.keySet()) {
            i++;

            if (alleles[i][0] == null)
                continue;

            for (int j = 0; j < alleles[i].length; j++) {
                if (!alleles[i][j].matches(validAlleleRegex))
                    throw new Exception("Invalid allele '" + alleles[i][j] + "' provided for " + sIndOrSp + " at variant" + variantToFeed.getId());

                if ("I".equals(alleles[i][j]))
                    alleles[i][j] = "NN";
                else if ("D".equals(alleles[i][j]))
                    alleles[i][j] = "N";

                Integer alleleIndex = alleleIndexMap.get(alleles[i][j]);
                if (alleleIndex != null)
                    continue;

                alleleIndex = variantToFeed.getKnownAlleles().size();
                variantToFeed.getKnownAlleles().add(alleles[i][j]);
                alleleIndexMap.put(alleles[i][j], alleleIndex);
            }

            try {
                Stream<String> alleleStream;
                if (alleles[i].length == 1 && m_ploidy > 1) {
                    LinkedList<String> alleleList = new LinkedList<>();
                    while (alleleList.size() < m_ploidy)
                        alleleList.add(alleles[i][0]);
                    alleleStream = alleleList.stream();
                } else
                    alleleStream = Arrays.stream(alleles[i]);

                SampleGenotype aGT = new SampleGenotype(alleleStream.map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/")));
                vrd.getSampleGenotypes().put(m_providedIdToCallsetMap.get(sIndOrSp).getId(), aGT);
            } catch (Exception e) {
                LOG.warn("Ignoring invalid genotype \"" + String.join("/", alleles[i]) + "\" for variant " + variantToFeed.getId() + " and individual " + sIndOrSp, e);
            }
        }

        if (nAssemblyId != null && fImportUnknownVariants && variantToFeed.getReferencePosition(nAssemblyId) == null && sequence != null)
            variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(sequence, bpPos, !variantToFeed.getKnownAlleles().isEmpty() ? bpPos + variantToFeed.getKnownAlleles().iterator().next().length() - 1 : null));

        if (!alleleIndexMap.isEmpty()) {
            Type variantType = nonSnpVariantTypeMap.get(variantToFeed.getId());
            String sVariantType = variantType == null ? Type.SNP.toString() : variantType.toString();
            if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType()))
                variantToFeed.setType(sVariantType);
            else if (null != variantType && Type.NO_VARIATION != variantType && !variantToFeed.getType().equals(sVariantType))
                throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());
        }
        
        if (project.getId() > 1 || project.getRuns().size() > 0)
            updateExistingVrdAlleles(mongoTemplate, initialAlleleCount, variantToFeed);

        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setPositions(variantToFeed.getPositions());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        return vrd;
    }

    protected HashMap<String, ArrayList<String>> checkSynonymGenotypeConsistency(File variantOrientedFile, HashMap<String, String> existingVariantIDs, Collection<String> individualsInProvidedOrder, String outputPathAndPrefix, HashSet<Integer> emptyLineIndexesToFill) throws IOException {
        long b4 = System.currentTimeMillis();
        LOG.info("Checking genotype consistency between synonyms...");
        String sLine = null;

        FileOutputStream inconsistencyFOS = new FileOutputStream(new File(outputPathAndPrefix + "-INCONSISTENCIES.txt"));
        HashMap<String /*existing variant id*/, ArrayList<String /*individual*/>> result = new HashMap<>();

        Map<String, List<Integer>> variantLinePositions = new HashMap<>();
        int nCurrentLinePos = 0;
        try (Scanner scanner = new Scanner(variantOrientedFile)) {
            while (scanner.hasNextLine()) {
                sLine = scanner.nextLine();
                String providedVariantName = sLine.substring(0, sLine.indexOf("\t"));
                String existingId = existingVariantIDs.get(providedVariantName.toUpperCase());
                if (existingId != null && !existingId.toString().startsWith("*")) {
                    List<Integer> variantLines = variantLinePositions.get(existingId);
                    if (variantLines == null) {
                        variantLines = new ArrayList<>();
                        variantLinePositions.put(existingId, variantLines);
                    }
                    variantLines.add(nCurrentLinePos);
                }
                nCurrentLinePos++;
            }
        }

        Map<String /*variant id */, List<Integer> /*corresponding line positions*/> synonymLinePositions = variantLinePositions.entrySet().stream().filter(entry -> variantLinePositions.get(entry.getKey()).size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        variantLinePositions.clear();

        HashMap<Integer, String> linesNeedingComparison = new HashMap<>();
        TreeSet<Integer> linesToReadForComparison = new TreeSet<>();
        synonymLinePositions.values().stream().forEach(varPositions -> linesToReadForComparison.addAll(varPositions));
        nCurrentLinePos = 0;
        try (Scanner scanner = new Scanner(variantOrientedFile)) {
            for (int nLinePos : linesToReadForComparison) {
                while (nCurrentLinePos <= nLinePos) {
                    sLine = scanner.nextLine();
                    nCurrentLinePos++;
                }
                linesNeedingComparison.put(nLinePos, sLine);
            }
        }

        for (String variantId : synonymLinePositions.keySet()) {
            HashMap<String /*genotype*/, HashSet<String> /*synonyms*/>[] individualGenotypeListArray = new HashMap[individualsInProvidedOrder.size()];
            List<Integer> linesToCompareForVariant = synonymLinePositions.get(variantId);
            boolean fFoundAnyNonEmptyLineForThisVariant = false;
            for (int nLineNumber = 0; nLineNumber < linesToCompareForVariant.size(); nLineNumber++) {
                String[] synAndGenotypes = linesNeedingComparison.get(linesToCompareForVariant.get(nLineNumber)).split("\t");
                boolean fFoundAnyGenotypeInThisLine = false;
                for (int individualIndex = 0; individualIndex < individualGenotypeListArray.length; individualIndex++) {
                    if (synAndGenotypes.length <= 1 + individualIndex)
                        break;

                    String genotype = synAndGenotypes[1 + individualIndex];
                    if (genotype.isEmpty())
                        continue;

                    if (individualGenotypeListArray[individualIndex] == null)
                        individualGenotypeListArray[individualIndex] = new HashMap<>();

                    fFoundAnyGenotypeInThisLine = true;
                    HashSet<String> synonymsWithThisGenotype = individualGenotypeListArray[individualIndex].get(genotype);
                    if (synonymsWithThisGenotype == null) {
                        synonymsWithThisGenotype = new HashSet<>();
                        individualGenotypeListArray[individualIndex].put(genotype, synonymsWithThisGenotype);
                    }
                    synonymsWithThisGenotype.add(synAndGenotypes[0]);
                }

                if (fFoundAnyGenotypeInThisLine)
                    fFoundAnyNonEmptyLineForThisVariant = true;
                else if (fFoundAnyNonEmptyLineForThisVariant)
                    emptyLineIndexesToFill.add(linesToCompareForVariant.get(nLineNumber));
            }

            Iterator<String> indIt = individualsInProvidedOrder.iterator();
            int individualIndex = 0;
            while (indIt.hasNext()) {
                String ind = indIt.next();
                HashMap<String, HashSet<String>> individualGenotypes = individualGenotypeListArray[individualIndex++];
                if (individualGenotypes != null && individualGenotypes.size() > 1) {
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