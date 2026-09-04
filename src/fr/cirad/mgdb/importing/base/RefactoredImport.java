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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.dao.OptimisticLockingFailureException;
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
        public static final VariantTask POISON_PILL = new VariantTask(null, null, null, null, null, false, false);

        final String line;
        final String providedVariantId;
        final String canonicalVariantId;
        final String sequence;
        final Long bpPosition;
        final boolean isExistingVariant;
        final boolean fRoutingKeyOnly;

        VariantTask(String line, String providedVariantId, String canonicalVariantId,
                    String sequence, Long bpPosition, boolean isExistingVariant, boolean fRoutingKeyOnly) {
            this.line = line;
            this.providedVariantId = providedVariantId;
            this.canonicalVariantId = canonicalVariantId;
            this.sequence = sequence;
            this.bpPosition = bpPosition;
            this.isExistingVariant = isExistingVariant;
            this.fRoutingKeyOnly = fRoutingKeyOnly;
        }
    }

    public long importTempFileContents(ProgressIndicator progress, int nNConcurrentThreads, MongoTemplate mongoTemplate, Integer nAssemblyId, File tempFile, LinkedHashMap<String, String> providedVariantPositions, HashMap<String, String> existingVariantIDs, GenotypingProject project, String sRun, HashMap<String, ArrayList<String>> inconsistencies, LinkedHashMap<String, String> orderedIndividualToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, HashSet<Integer> indexesOfLinesThatMustBeSkipped, boolean fSkipMonomorphic) throws Exception {
        String[] individuals = orderedIndividualToPopulationMap.keySet().toArray(new String[orderedIndividualToPopulationMap.size()]);
        final AtomicInteger totalParsedVariantCount = new AtomicInteger(0);
        final AtomicInteger totalWrittenVariantCount = new AtomicInteger(0);
        final AtomicInteger ignoredVariants = new AtomicInteger(0);

        try {
            String info = "Importing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            progress.setPercentageEnabled(true);

            final int nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / Math.max(1, individuals.length));
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

            // --- SINGLE UP-FRONT RESOLUTION PASS ---
            //
            // Resolve every provided variant id exactly once, sequentially, before starting the
            // dispatcher/workers. This serves two purposes:
            //  1. It replaces the per-line resolveVariantInfo() call the dispatcher used to make: since
            //     resolveVariantInfo() can mutate nonSnpVariantTypeMap, calling it more than once per
            //     variant would be both wasteful and, in principle, order-sensitive. Resolving once and
            //     caching the result avoids that entirely.
            //  2. It lets us determine, for free, exactly which routing keys are touched by MORE THAN ONE
            //     provided id - i.e. which variants can actually be revisited later in the file (either
            //     because several provided names resolve to the same pre-existing DB variant, or because
            //     several NEW variants share the same chromosome/position). Only those variants need the
            //     openVariants/resolvedPersistedIds/flushedVariantIds bookkeeping introduced to correctly
            //     survive being split across two chunks; every other (typically the vast majority of)
            //     variant is touched exactly once and can take a simplified, cheaper path with no risk of
            //     ever needing to be found again. Note this is strictly more complete than reusing
            //     checkSynonymGenotypeConsistency()'s own synonym detection would be: that method (only run
            //     when fCheckConsistencyBetweenSynonyms is enabled) only ever looks at synonymy against
            //     pre-existing DB variants, not at newly-introduced position-based synonyms - which is
            //     exactly the case that caused the DuplicateKeyException this mechanism exists to prevent.
            Map<String, ResolvedVariantInfo> resolvedInfoByProvidedId = new HashMap<>(providedVariantPositions.size() * 2);
            HashSet<String> knownSynonymRoutingKeys = new HashSet<>();
            {
                HashMap<String, String> firstProvidedIdForRoutingKey = new HashMap<>(providedVariantPositions.size() * 2);
                for (String providedVariantId : providedVariantPositions.keySet()) {
                    ResolvedVariantInfo resolvedInfo = resolveVariantInfo(
                        providedVariantId,
                        providedVariantPositions,
                        existingVariantIDs,
                        nonSnpVariantTypeMap,
                        m_fImportUnknownVariants
                    );
                    resolvedInfoByProvidedId.put(providedVariantId, resolvedInfo);

                    if (resolvedInfo.canonicalVariantId == null)
                        continue;

                    String firstOwner = firstProvidedIdForRoutingKey.putIfAbsent(resolvedInfo.canonicalVariantId, providedVariantId);
                    if (firstOwner != null)
                        knownSynonymRoutingKeys.add(resolvedInfo.canonicalVariantId);
                }
            }
            if (!knownSynonymRoutingKeys.isEmpty())
                LOG.debug(knownSynonymRoutingKeys.size() + " variant(s) have synonymous entries in this import and will be tracked accordingly");

            // --- DISPATCHER + QUEUE IMPLEMENTATION ---

            // Create worker queues
            int nImportThreads = Math.max(1, (nNConcurrentThreads - 1) / 2);
            @SuppressWarnings("unchecked")
            BlockingQueue<VariantTask>[] workerQueues = new BlockingQueue[nImportThreads];
            for (int i = 0; i < nImportThreads; i++) {
                workerQueues[i] = new LinkedBlockingQueue<>();
            }

            // Arbitrates the persisted (final) _id across DIFFERENT routing-key groups, e.g. the same
            // provided marker name appearing with inconsistent/conflicting positions on different lines
            // of the file, which would otherwise create two distinct VariantData objects sharing the
            // same final _id and blow up at insert time with a DuplicateKeyException. This is unrelated
            // to knownSynonymRoutingKeys above (that set is about the SAME routing key being touched
            // twice; this handles two DIFFERENT routing keys colliding on the same persisted id) and
            // must remain shared/checked unconditionally, and across all workers.
            ConcurrentHashMap<String, String> claimedPersistedIds = new ConcurrentHashMap<>();

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
                                totalParsedVariantCount,
                                totalWrittenVariantCount,
                                providedVariantPositions.size(),
                                inconsistencies,
                                m_maxExpectedAlleleCount,
                                ignoredVariants,
                                existingVariantIDs,
                                fSkipMonomorphic,
                                m_ploidy,
                                m_fImportUnknownVariants,
                                claimedPersistedIds,
                                knownSynonymRoutingKeys
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

                            // Reuse the resolution computed once in the up-front pass above, instead of
                            // calling resolveVariantInfo() again here.
                            ResolvedVariantInfo resolvedInfo = resolvedInfoByProvidedId.get(providedVariantId);

                            if (resolvedInfo.canonicalVariantId == null && !m_fImportUnknownVariants) {
                                ignoredVariants.incrementAndGet();
                                continue;
                            }

                            // Only applies to variants not already in DB (existing ones are never "to be discovered" as monomorphic)
                            if (fSkipMonomorphic && !resolvedInfo.isExistingVariant) {
                                String[] distinctGTs = Arrays.stream(splitLine, 1, splitLine.length)
                                    .filter(gt -> !gt.isEmpty())
                                    .distinct()
                                    .toArray(String[]::new);
                                if (distinctGTs.length == 0 ||
                                    (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2)) {
                                    continue;
                                }
                            }

                            // resolvedInfo.canonicalVariantId is never null here: either it's an existing id,
                            // or m_fImportUnknownVariants is true and resolveVariantInfo always produced one
                            String idToDispatch = resolvedInfo.canonicalVariantId;
                            int workerIndex = Math.floorMod(idToDispatch.hashCode(), nImportThreads);

                            VariantTask task = new VariantTask(
                                line,
                                providedVariantId,
                                resolvedInfo.canonicalVariantId,
                                resolvedInfo.sequence,
                                resolvedInfo.bpPosition,
                                resolvedInfo.isExistingVariant,
                                resolvedInfo.fRoutingKeyOnly
                            );

                            workerQueues[workerIndex].put(task);
                        }

                        // Send poison pills to all workers
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

            if (ignoredVariants.get() > 0)
                LOG.warn("Number of ignored variants: " + ignoredVariants);

            if (progress.getError() != null || progress.isAborted())
                return totalParsedVariantCount.get();

            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);
        } finally {
            // cleanup
        }
        return totalParsedVariantCount.get();
    }

    /**
     * Worker method that processes variant tasks.
     *
     * Memory footprint is bounded regardless of file size: only variants known (per knownSynonymRoutingKeys,
     * computed once up front) to be touched by more than one line in this run go through the
     * openVariants/resolvedPersistedIds/flushedVariantIds bookkeeping - which lets a variant be correctly
     * found again after being flushed and evicted, at the cost of an extra findById on revisit. Every other
     * (typically the vast majority of) variant is touched exactly once and takes a simplified path: no
     * bookkeeping entry is ever created for it, since it is mathematically guaranteed to never be revisited.
     *
     * None of the per-worker maps below need to be thread-safe: a given routing key is always dispatched to
     * exactly one worker (deterministic hash-based routing in the dispatcher above), so no other thread ever
     * touches these entries.
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
            AtomicInteger totalParsedVariantCount,
            AtomicInteger totalWrittenVariantCount,
            int totalVariants,
            HashMap<String, ArrayList<String>> inconsistencies,
            Integer maxExpectedAlleleCount,
            AtomicInteger ignoredVariants,
            HashMap<String, String> existingVariantIDs,
            boolean fSkipMonomorphic,
            int ploidy,
            boolean importUnknownVariants,
            ConcurrentHashMap<String, String> claimedPersistedIds,
            HashSet<String> knownSynonymRoutingKeys) throws Exception {

        // routing key -> currently open (not yet flushed) object, ONLY for routing keys known to have
        // synonymous entries (knownSynonymRoutingKeys) - variants touched exactly once never need an entry.
        Map<String, VariantData> openVariants = new HashMap<>();
        // routing key -> persisted _id chosen for it, ONLY for synonym-candidate routing keys.
        Map<String, String> resolvedPersistedIds = new HashMap<>();
        // persisted _id -> already flushed by this worker, ONLY for synonym-candidate routing keys.
        Map<String, Boolean> flushedVariantIds = new HashMap<>();

        // routing key -> object, for the chunk currently being accumulated
        HashMap<String, VariantData> unsavedVariants = new HashMap<>();
        HashMap<String, VariantData> variantsToResave = new HashMap<>();
        HashSet<VariantRunData> unsavedRuns = new HashSet<>();
        int workerProcessed = 0;

        while (true) {
            VariantTask task = queue.take();
            if (task == VariantTask.POISON_PILL || progress.getError() != null || progress.isAborted()) {
                break;
            }

            String variantId = task.canonicalVariantId != null ? task.canonicalVariantId : task.providedVariantId;

            if (variantId != null && variantId.startsWith("*")) {
                LOG.warn("Skipping deprecated variant data: " + task.providedVariantId);
                continue;
            }

            boolean fMayHaveSynonyms = knownSynonymRoutingKeys.contains(variantId);

            VariantData variant = fMayHaveSynonyms ? openVariants.get(variantId) : null;
            if (variant == null) {
                String persistedId = fMayHaveSynonyms ? resolvedPersistedIds.get(variantId) : null;
                if (persistedId != null) {
                    // This routing key was already resolved earlier in this run, but the object has since
                    // been flushed and evicted from memory - fetch it back to append to it.
                    variant = mongoTemplate.findById(persistedId, VariantData.class);
                    if (variant == null)
                        throw new Exception("Variant \"" + persistedId + "\" was expected to already exist in the "
                            + "database (it was flushed earlier in this very run) but could not be found");
                } else if (task.isExistingVariant) {
                    // Known to exist in DB before this run started (existingVariantIDs is never updated
                    // live), so findById is guaranteed to find it.
                    variant = mongoTemplate.findById(variantId, VariantData.class);
                    if (fMayHaveSynonyms)
                        resolvedPersistedIds.put(variantId, variantId);
                } else {
                    // Check if we're allowed to create new variants
                    if (!importUnknownVariants) {
                        LOG.debug("Skipping unknown variant: " + variantId);
                        continue;
                    }

                    // Choose the id that will actually be PERSISTED: the id provided in the file takes
                    // priority; the chr_pos routing key is never written as-is (it only exists to group
                    // synonyms together).
                    String persistedIdToUse;
                    if (isValidProvidedId(task.providedVariantId))
                        persistedIdToUse = task.providedVariantId;
                    else if (task.fRoutingKeyOnly)
                        persistedIdToUse = generateFallbackVariantId();
                    else
                        persistedIdToUse = task.canonicalVariantId;

                    // Atomically claim the persisted id: prevents two DIFFERENT routing-key groups from
                    // ending up as two distinct VariantData objects sharing the same final _id. Always
                    // checked, regardless of fMayHaveSynonyms (see comment where claimedPersistedIds is
                    // created).
                    String claimOwner = claimedPersistedIds.putIfAbsent(persistedIdToUse, variantId);
                    if (claimOwner != null && !claimOwner.equals(variantId)) {
                        LOG.warn("Provided ID \"" + task.providedVariantId + "\" is associated with inconsistent data "
                            + "across the import file (routing key " + variantId + " conflicts with routing key " + claimOwner
                            + ", which already claimed this ID) - generating a distinct ID instead. Please check for "
                            + "duplicate/conflicting entries for this marker.");
                        persistedIdToUse = generateFallbackVariantId();
                        claimedPersistedIds.put(persistedIdToUse, variantId);
                    }

                    String id = ObjectId.isValid(persistedIdToUse) ? "_" + persistedIdToUse : persistedIdToUse;
                    variant = new VariantData(id);
                    if (fMayHaveSynonyms)
                        resolvedPersistedIds.put(variantId, persistedIdToUse);
                }
                if (fMayHaveSynonyms)
                    openVariants.put(variantId, variant);
            }

            // If the id provided for this line isn't the one that ended up as the persisted _id, keep it
            // as an internal synonym instead of silently dropping it. Nothing to do for already-existing
            // variants: the provided id was already a known synonym.
            if (!task.isExistingVariant && isValidProvidedId(task.providedVariantId)) {
                String normalizedProvidedId = ObjectId.isValid(task.providedVariantId) ? "_" + task.providedVariantId : task.providedVariantId;
                if (!normalizedProvidedId.equals(variant.getId()))
                    addInternalSynonym(variant, task.providedVariantId);
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
                        fInconsistentData = inconsistentIndividuals != null && inconsistentIndividuals.contains(individuals[nIndividualIndex]);
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
                // Route to the resave map if this exact _id has already been flushed once during this run
                // - otherwise it goes through the normal insert-or-save path used for variants touched for
                // the first time in this run. Non-synonym-candidate variants can never hit the resave path
                // (flushedVariantIds is never populated for them, since fMayHaveSynonyms gates every write
                // to it below), which is exactly the simplification this whole mechanism is meant to buy.
                if (fMayHaveSynonyms && flushedVariantIds.containsKey(variant.getId()))
                    variantsToResave.put(variantId, variant);
                else
                    unsavedVariants.put(variantId, variant);
                unsavedRuns.add(runToSave);

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

            workerProcessed++;
            int newCount = totalParsedVariantCount.incrementAndGet();

            // Save based on per-worker processed count to keep chunks consistent
            if (workerProcessed % chunkSize == 0 && (!unsavedVariants.isEmpty() || !variantsToResave.isEmpty())) {
                flushChunk(mongoTemplate, existingVariantIDs, unsavedVariants, variantsToResave, unsavedRuns, flushedVariantIds, openVariants, knownSynonymRoutingKeys);
                progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size() + variantsToResave.size()) * 100 / totalVariants);

                unsavedVariants = new HashMap<>();
                variantsToResave = new HashMap<>();
                unsavedRuns = new HashSet<>();
            }

            if (newCount % (chunkSize * 50) == 0) {
                LOG.debug(newCount + " lines processed by worker");
            }
        }

        // Save remaining
        if (!unsavedVariants.isEmpty() || !variantsToResave.isEmpty()) {
            flushChunk(mongoTemplate, existingVariantIDs, unsavedVariants, variantsToResave, unsavedRuns, flushedVariantIds, openVariants, knownSynonymRoutingKeys);
            progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size() + variantsToResave.size()) * 100 / totalVariants);
        }
    }

    /**
     * Flushes one chunk to the DB, then evicts the flushed objects from openVariants (only ever populated
     * for synonym-candidate routing keys) so their memory can be reclaimed. Variants already flushed
     * earlier in this run are saved (never re-inserted); variants touched for the first time are inserted
     * (or saved, if the DB already contained variants before this run started).
     */
    private void flushChunk(MongoTemplate mongoTemplate, HashMap<String, String> existingVariantIDs,
            HashMap<String, VariantData> unsavedVariants, HashMap<String, VariantData> variantsToResave,
            HashSet<VariantRunData> unsavedRuns, Map<String, Boolean> flushedVariantIds,
            Map<String, VariantData> openVariants, HashSet<String> knownSynonymRoutingKeys) throws InterruptedException {
        for (VariantData vd : variantsToResave.values()) {
            try {
                mongoTemplate.save(vd);
            } catch (OptimisticLockingFailureException olfe) {
                mongoTemplate.save(vd);
            }
        }

        if (!unsavedVariants.isEmpty() || !unsavedRuns.isEmpty())
            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants.values(), unsavedRuns);

        for (Map.Entry<String, VariantData> entry : unsavedVariants.entrySet()) {
            if (knownSynonymRoutingKeys.contains(entry.getKey())) {
                flushedVariantIds.put(entry.getValue().getId(), Boolean.TRUE);
                openVariants.remove(entry.getKey());
            }
        }
        for (String routingKey : variantsToResave.keySet())
            openVariants.remove(routingKey);
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