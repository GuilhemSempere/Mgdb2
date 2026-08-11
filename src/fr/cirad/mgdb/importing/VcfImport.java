package fr.cirad.mgdb.importing;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import fr.cirad.mgdb.model.mongo.maintypes.*;
import fr.cirad.tools.mongo.MongoTemplateManager;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.bulk.BulkWriteResult;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.importing.parameters.VCFParameters;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
//import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
//import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
//import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.CloseableTribbleIterator;
import htsjdk.tribble.FeatureCodec;
import htsjdk.tribble.FeatureReader;
import htsjdk.tribble.TribbleIndexedFeatureReader;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.LineIterator;
import htsjdk.tribble.readers.LineIteratorImpl;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataV3Id;

public class VcfImport extends AbstractGenotypeImport<VCFParameters> {

    private static final Logger LOG = Logger.getLogger(VariantData.class);

    public static final String ANNOTATION_FIELDNAME_EFF = "EFF";
    public static final String ANNOTATION_FIELDNAME_ANN = "ANN";
    public static final String ANNOTATION_FIELDNAME_CSQ = "CSQ";

    @SuppressWarnings({"unchecked", "rawtypes"})
    private FeatureReader<VariantContext> reader;
    private Iterator<VariantContext> variantIterator;

    public VcfImport() {
        this("random_process_" + System.currentTimeMillis() + "_" + Math.random());
    }

    public VcfImport(boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this();
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    public VcfImport(String processID) {
        m_processID = processID;
    }

    public VcfImport(String processID, boolean fCloseContextAfterImport) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, VCF file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list)");

        File mainFile = new File(args[4]);
        if (!mainFile.exists() || mainFile.length() == 0) {
            throw new Exception("File " + args[4] + " is missing or empty!");
        }

        int mode = 0;
        try {
            mode = Integer.parseInt(args[6]);
        } catch (Exception e) {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }

        VCFParameters params = new VCFParameters(
                args[0], //sModule
                args[1], //sProject
                args[2], //sRun
                args[3], //sTechnology
                null, // nPloidy
                args[5], //assemblyName
                null, //sampleToIndividualMap
                false,//fSkipMonomorphic
                mode, //importMode,
                false, //fisBCF
                new File(args[4]).toURI().toURL()
        );
        new VcfImport().importToMongo(params);
    }

    /**
     * Task class for dispatching variant processing
     * MINIMAL - just carries the raw VariantContextHologram and the ID for routing
     */
    private static class VariantTask {
        public static final VariantTask POISON_PILL = new VariantTask(null, null);

        final VariantContextHologram vcfEntry;
        final String providedVariantId;  // For routing only - worker resolves canonical ID

        VariantTask(VariantContextHologram vcfEntry, String providedVariantId) {
            this.vcfEntry = vcfEntry;
            this.providedVariantId = providedVariantId;
        }
    }

    @Override
    protected long doImport(VCFParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer projectId) throws Exception {
        String sModule = params.getModule();
        String sProject = params.getProject();
        String sRun = params.getRun();
        List<String> runs = project.getRuns();
        int runIndex = runs.indexOf(params.getRun());
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();

        final Map<String, Integer> genotypeCodeCache = new ConcurrentHashMap<>();

        VCFHeader header = (VCFHeader) reader.getHeader();
        int effectAnnotationPos = -1, geneIdAnnotationPos = -1;
        for (VCFInfoHeaderLine headerLine : header.getInfoHeaderLines()) {
            if (ANNOTATION_FIELDNAME_EFF.equals(headerLine.getID()) || ANNOTATION_FIELDNAME_ANN.equals(headerLine.getID()) || ANNOTATION_FIELDNAME_CSQ.equals(headerLine.getID())) {
                String desc = headerLine.getDescription().replaceAll("\\(", "").replaceAll("\\)", "");
                desc = desc.substring(1 + desc.indexOf(":")).replace("'", "");
                String[] fields = desc.split("\\|");
                for (int i = 0; i < fields.length; i++) {
                    String trimmedField = fields[i].trim();
                    if (/*EFF*/ "Gene_Name".equals(trimmedField) || /*EFF*/ "Gene_ID".equals(trimmedField) || /*CSQ or ANN*/ "Gene".equals(trimmedField)) {
                        geneIdAnnotationPos = i;
                    } else if (/*EFF*/ "Annotation".equals(trimmedField) || /*CSQ or ANN*/ "Consequence".equals(trimmedField)) {
                        effectAnnotationPos = i;
                    }
                }
            }
        }

        for (VCFContigHeaderLine contigLine : header.getContigLines()) {
            Map<String, String> lineFields = contigLine.getGenericFields();
            String sAssembly = lineFields.get("assembly"), sLength = lineFields.get("length");
            if (sAssembly != null || sLength != null) {
                Sequence seq = new Sequence(contigLine.getID());
                seq.setAssembly(sAssembly);
                if (sLength != null)
                    seq.setLength(Long.parseLong(sLength));
                mongoTemplate.save(seq);
            }
        }

        mongoTemplate.save(new DBVCFHeader(new VcfHeaderId(project.getId(), sRun), header));

        progress.addStep("Header was written for project " + sProject + " and run " + sRun);
        progress.moveToNextStep();
        LOG.info(progress.getProgressDescription());

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

        variantIterator = reader.iterator();
        progress.addStep("Processing variant lines");
        progress.moveToNextStep();

        AtomicInteger totalParsedVariantCount = new AtomicInteger(0);
        AtomicInteger totalWrittenVariantCount = new AtomicInteger(0);
        String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());

        int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        int nImportThreads = Math.max(1, (int)(nNConcurrentThreads * 0.60));

        @SuppressWarnings("unchecked")
        BlockingQueue<VariantTask>[] workerQueues = new BlockingQueue[nImportThreads];
        for (int i = 0; i < nImportThreads; i++) {
            workerQueues[i] = new LinkedBlockingQueue<>(5000); // Large queue for workers
        }

        final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
        if (assemblyIDs.isEmpty())
            assemblyIDs.add(null);

        final GenotypingProject finalProject = project;
        final MongoTemplate finalMongoTemplate = mongoTemplate;
        final Integer nAssemblyId = assembly == null ? null : assembly.getId();
        final int finalEffectAnnotationPos = effectAnnotationPos;
        final int finalGeneIdAnnotationPos = geneIdAnnotationPos;
        HashMap<String, Comparable> phasingGroups = new HashMap<>();
        HashSet<String> distinctEncounteredGeneNames = new HashSet<>();

        createCallSetsSamplesIndividuals(header.getSampleNamesInOrder(), mongoTemplate, project.getId(), sRun, sampleToIndividualMap, progress);
        final List<String> sampleNamesInOrder = header.getSampleNamesInOrder();
        setSamplesPersisted(true);

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
                            nAssemblyId,
                            finalProject,
                            sRun,
                            assemblyIDs,
                            progress,
                            totalParsedVariantCount,
                            totalWrittenVariantCount,
                            existingVariantIDs,
                            fSkipMonomorphic,
                            header,
                            finalEffectAnnotationPos,
                            finalGeneIdAnnotationPos,
                            phasingGroups,
                            distinctEncounteredGeneNames,
                            generatedIdBaseString,
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
            int chunkSize = 0;
            int totalDispatched = 0;

            while (variantIterator.hasNext() && progress.getError() == null && !progress.isAborted()) {
                VariantContext vcfEntry = variantIterator.next();

                if (vcfEntry.getCommonInfo().hasAttribute(""))
                    vcfEntry.getCommonInfo().removeAttribute("");

                VariantContextHologram hologram = new VariantContextHologram(vcfEntry);

                if (chunkSize == 0) {
                    chunkSize = (int) (vcfEntry.getSampleNames().isEmpty() ?
                        nMaxChunkSize :
                        Math.max(1, Math.ceil((float) nMaxChunkSize / vcfEntry.getSampleNames().size())));
                    LOG.info("Importing by chunks of size " + chunkSize);
                }

                // --- MINIMAL DISPATCHER: Just route based on ID ---
                String providedId = hologram.hasID() && !".".equals(hologram.getID()) && !hologram.getID().isEmpty() ?
                    hologram.getID() :
                    hologram.getContig() + "_" + hologram.getStart();

                workerQueues[Math.floorMod(providedId.hashCode(), nImportThreads)].put(new VariantTask(hologram, providedId));
            }

            for (BlockingQueue<VariantTask> queue : workerQueues)
                queue.put(VariantTask.POISON_PILL);

            LOG.info("Dispatcher finished: " + totalDispatched + " variants dispatched");

        } catch (Exception e) {
            progress.setError("Dispatcher failed: " + e.getMessage());
            LOG.error(progress.getError(), e);
        }

        // Wait for workers to finish
        for (int i = 0; i < nImportThreads; i++)
            importThreads[i].join();

        reader.close();

        if (progress.getError() != null || progress.isAborted())
            return 0;

        // Build gene list cache
        if (!distinctEncounteredGeneNames.isEmpty()) {
            progress.addStep("Building gene list cache");
            progress.moveToNextStep();

            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, MgdbDao.COLLECTION_NAME_GENE_CACHE);
            for (String geneName : distinctEncounteredGeneNames) {
                Update update = new Update();
                update.addToSet(Run.FIELDNAME_PROJECT_ID, project.getId());
                bulkOperations.upsert(new Query(Criteria.where("_id").is(geneName)), update);
            }

            BulkWriteResult wr = bulkOperations.execute();
            if (wr.getUpserts().size() > 0)
                LOG.info("Database " + sModule + ": " + wr.getUpserts().size() + " documents inserted into " + MgdbDao.COLLECTION_NAME_GENE_CACHE);
            if (wr.getModifiedCount() > 0)
                LOG.info("Database " + sModule + ": " + wr.getModifiedCount() + " documents updated in " + MgdbDao.COLLECTION_NAME_GENE_CACHE);
        }

        return totalParsedVariantCount.get();
    }

    /**
     * Worker method that processes variant tasks
     * Saves based on accumulated variants, not a counter
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
            VCFHeader header,
            int effectAnnotationPos,
            int geneIdAnnotationPos,
            HashMap<String, Comparable> phasingGroups,
            HashSet<String> distinctEncounteredGeneNames,
            String generatedIdBaseString,
            int projectID,
            int runIndex) throws Exception {

        HashSet<VariantData> unsavedVariants = new HashSet<>();
        HashSet<VariantRunData> unsavedRuns = new HashSet<>();
        HashMap<String, VariantData> variantCache = new HashMap<>();

        int sampleCount = header.getSampleNamesInOrder().size();
        int chunkSize = Math.max(1, Math.min(1000, (int) Math.ceil((float) nMaxChunkSize / Math.max(1, sampleCount))));

        while (true) {
            VariantTask task = queue.take();
            if (task == VariantTask.POISON_PILL || progress.getError() != null || progress.isAborted())
                break;

            VariantContextHologram hologram = task.vcfEntry;
			String variantId = null;
			boolean hasValidId = hologram.hasID() && !".".equals(hologram.getID()) && !hologram.getID().isEmpty();

			List<String> idAndSynonyms = hasValidId ? Arrays.asList(new String[]{hologram.getID()}) : null;
			try {
				for (String variantDescForPos : getIdentificationStrings(hologram.getType().toString(), hologram.getContig(), (long) hologram.getStart(), idAndSynonyms)) {
			        variantId = existingVariantIDs.get(variantDescForPos);
			        if (variantId != null)
			        	break;
				}
			} catch (Exception e) {		// No position and no ID - will create new variant
			    LOG.debug("Cannot build identification strings: " + e.getMessage());
			}

			if (variantId == null) {	// Not found, create new variant
			    if (hasValidId)
			        variantId = (ObjectId.isValid(hologram.getID()) ? "_" : "") + hologram.getID();
			    else
			        variantId = generatedIdBaseString + String.format("%09x", totalParsedVariantCount.getAndIncrement());
			}

            // Skip monomorphic (only for new variants)
            if (fSkipMonomorphic && !existingVariantIDs.containsKey(variantId)) {
                String[] distinctGTs = StreamSupport.stream(
                    java.util.Spliterators.spliteratorUnknownSize(
                        hologram.getGenotypesOrderedByName().iterator(),
                        java.util.Spliterator.ORDERED), false)
                    .map(gt -> gt.getGenotypeString())
                    .filter(gt -> gt.charAt(0) != '.')
                    .distinct()
                    .toArray(String[]::new);
                if (distinctGTs.length == 0 ||
                    (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2)) {
                    continue;
                }
            }

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

            // Add run and process
            variant.getRuns().add(new Run(project.getId(), sRun));

            VariantRunData runToSave = addVcfDataToVariant(
                mongoTemplate,
                header,
                variant,
                nAssemblyId,
                hologram,
                project,
                sRun,
                phasingGroups,
                effectAnnotationPos,
                geneIdAnnotationPos,
                distinctEncounteredGeneNames
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

            if (unsavedVariants.size() >= chunkSize) {
                persistVariantsAndGenotypesV3(!existingVariantIDs.isEmpty(), mongoTemplate,
                    unsavedVariants, unsavedRuns, projectID, runIndex);
                progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size()));

                variantCache.clear();
                unsavedVariants = new HashSet<>();
                unsavedRuns = new HashSet<>();
            }

        }

        // Save remaining
        if (!unsavedVariants.isEmpty()) {
            persistVariantsAndGenotypesV3(!existingVariantIDs.isEmpty(), mongoTemplate,
                unsavedVariants, unsavedRuns, projectID, runIndex);
            progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size()));
        }
    }

    private static class SeekableByteChannelAdapter implements SeekableByteChannel {
        private final InputStream inputStream;
        private long position = 0;
        private boolean open = true;
        
        public SeekableByteChannelAdapter(InputStream inputStream) {
            this.inputStream = inputStream;
        }
        
        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) throw new java.nio.channels.ClosedChannelException();
            
            int remaining = dst.remaining();
            if (remaining == 0) return 0;
            
            byte[] buffer = new byte[remaining];
            int bytesRead = inputStream.read(buffer);
            
            if (bytesRead > 0) {
                dst.put(buffer, 0, bytesRead);
                position += bytesRead;
            }
            
            return bytesRead;
        }
        
        @Override
        public int write(ByteBuffer src) throws IOException {
            throw new UnsupportedOperationException("Write not supported");
        }
        
        @Override
        public long position() throws IOException {
            return position;
        }
        
        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
            if (newPosition != position) {
                throw new UnsupportedOperationException("Seeking not supported on decompressed BGZF stream");
            }
            return this;
        }
        
        @Override
        public long size() throws IOException {
            return -1;
        }
        
        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            throw new UnsupportedOperationException("Truncate not supported");
        }
        
        @Override
        public boolean isOpen() {
            return open;
        }
        
        @Override
        public void close() throws IOException {
            if (open) {
                open = false;
                inputStream.close();
            }
        }
    }

    private static class NonSeekableByteChannel implements SeekableByteChannel {
        private final InputStream inputStream;
        private long position = 0;
        private boolean open = true;
        
        public NonSeekableByteChannel(InputStream inputStream) {
            this.inputStream = inputStream;
        }
        
        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) throw new java.nio.channels.ClosedChannelException();
            
            int remaining = dst.remaining();
            if (remaining == 0) return 0;
            
            byte[] buffer = new byte[remaining];
            int bytesRead = inputStream.read(buffer);
            
            if (bytesRead > 0) {
                dst.put(buffer, 0, bytesRead);
                position += bytesRead;
            }
            
            return bytesRead;
        }
        
        @Override
        public int write(ByteBuffer src) throws IOException {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public long position() throws IOException {
            return position;
        }
        
        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
            if (newPosition != position) {
                throw new UnsupportedOperationException("Cannot seek on decompressed stream");
            }
            return this;
        }
        
        @Override
        public long size() throws IOException {
            return -1;
        }
        
        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public boolean isOpen() {
            return open;
        }
        
        @Override
        public void close() throws IOException {
            if (open) {
                open = false;
                inputStream.close();
            }
        }
    }
    public static void saveChunkV3(final Collection<VariantData> unsavedVariants, final Collection<VariantRunData> unsavedRuns, HashMap<String, String> existingVariantIDs, MongoTemplate finalMongoTemplate, ProgressIndicator progress, ExecutorService saveService, int projectIndex, int runIndex) throws InterruptedException {
        if (progress.getError() != null || progress.isAborted())
            return;

        Thread insertionThread = new Thread() {
            @Override
            public void run() {
                try {
                    persistVariantsAndGenotypesV3(!existingVariantIDs.isEmpty(), finalMongoTemplate, unsavedVariants, unsavedRuns, projectIndex, runIndex);
                } catch (InterruptedException e) {
                    progress.setError(e.getMessage());
                    LOG.error(e);
                }
            }
        };
        saveService.execute(insertionThread);
    }

    public static void persistVariantsAndGenotypesV3(boolean fDBAlreadyContainsVariants, MongoTemplate mongoTemplate,
                                                     Collection<VariantData> unsavedVariants, Collection<VariantRunData> unsavedRuns,
                                                     int projectIndex, int runIndex) throws InterruptedException {

        long b4 = System.currentTimeMillis();

        Thread vdAsyncThread = new Thread() {
            public void run() {
                if (!fDBAlreadyContainsVariants) {
                    mongoTemplate.insert(unsavedVariants, VariantData.class);
                } else {
                    for (VariantData vd : unsavedVariants) {
                        try {
                            mongoTemplate.save(vd);
                        } catch (OptimisticLockingFailureException olfe) {
                            mongoTemplate.save(vd);
                        }
                    }
                }
            }
        };
        vdAsyncThread.start();

        List<VariantRunData> syncList = new ArrayList<>(), asyncList = new ArrayList<>();
        int i = 0;
        for (VariantRunData vrd : unsavedRuns)
            (i++ < unsavedRuns.size() / 2 ? syncList : asyncList).add(vrd);

        AtomicReference<DuplicateKeyException> asyncException = new AtomicReference<>();
        Thread vrdAsyncThread = new Thread() {
            public void run() {
                if (!fDBAlreadyContainsVariants) {
                    try {
                        mongoTemplate.insert(asyncList, VariantRunData.class);
                    } catch (DuplicateKeyException dke) {
                        asyncException.set(dke);
                    }
                } else {
                    asyncList.forEach(vrd -> upsertV3(vrd, projectIndex, runIndex, mongoTemplate));
                }
            }
        };
        vrdAsyncThread.start();

        if (!fDBAlreadyContainsVariants) {
            try {
                mongoTemplate.insert(syncList, VariantRunData.class);
                vrdAsyncThread.join();
                if (asyncException.get() != null)
                    throw asyncException.get();
            } catch (DuplicateKeyException dke) {
                LOG.info("Persisting runs using upsert() because of synonym variants: " + dke.getMessage());
                // re-spawn async thread to upsert its list too
                Thread vrdAsyncFallbackThread = new Thread() {
                    public void run() {
                        asyncList.forEach(vrd -> upsertV3(vrd, projectIndex, runIndex, mongoTemplate));
                    }
                };
                vrdAsyncFallbackThread.start();
                syncList.forEach(vrd -> upsertV3(vrd, projectIndex, runIndex, mongoTemplate));
                vrdAsyncFallbackThread.join();
            }
        } else {
            syncList.forEach(vrd -> upsertV3(vrd, projectIndex, runIndex, mongoTemplate));
            vrdAsyncThread.join();
        }

        vdAsyncThread.join();
        //LOG.info("persistVariantsAndGenotypes took: " + (System.currentTimeMillis() - b4) + "ms for " + unsavedRuns.size() + " docs");
    }

    private static void upsertV3(VariantRunData vrd, int projectIndex, int runIndex, MongoTemplate mongoTemplate) {
        String id = vrd.getIdV3();
        Query query = new Query(Criteria.where("_id").is(id));

        List<Integer> newGenotypeArray = vrd.getGenotypeArray().get(projectIndex).get(runIndex);

        List<HashMap<String, Object>> newGenotypeAnnotationArray = null;
        if (vrd.getGenotypeAnnotationArray()!=null && !vrd.getGenotypeAnnotationArray().isEmpty()) {
            newGenotypeAnnotationArray = vrd.getGenotypeAnnotationArray().get(projectIndex).get(runIndex);
        }

        HashMap<String, Object> newVariantRunAnnotationArray = null;
        if (vrd.getVariantRunAnnotation()!=null && !vrd.getVariantRunAnnotation().isEmpty()) {
            newVariantRunAnnotationArray = vrd.getVariantRunAnnotation().get(projectIndex).get(runIndex);
        }

        Map<String, Object> newVariantAnnotation = vrd.getVariantAnnotation();


        // Paths where new data will be set
        // FIXME: Make sure to use variables instead.
        String spPath = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + projectIndex + "." + runIndex;
        String aiPath = VariantRunData.SECTION_ADDITIONAL_INFO + "." + projectIndex + "." + runIndex;
        String vraPath = "vra" + "." + projectIndex + "." + runIndex;

        if (!mongoTemplate.exists(query, VariantRunData.class)) {
            try {
                VariantRunData empty = new VariantRunData();
                empty.setId(vrd.getIdV3());
                empty.setKnownAlleles(vrd.getKnownAlleles());
                empty.setType(vrd.getType());
                mongoTemplate.insert(empty);
            } catch (org.springframework.dao.DuplicateKeyException dke) {
                // race condition, fall through to update
            }
        }

        Update update = new Update()
                .set(VariantRunData.FIELDNAME_KNOWN_ALLELES, vrd.getKnownAlleles())
                .set(VariantRunData.FIELDNAME_TYPE,          vrd.getType())
                .set(VariantRunData.FIELDNAME_POSITIONS,     vrd.getPositions())
                .set(spPath, newGenotypeArray);

        // initialize sp project slot if missing
        mongoTemplate.updateFirst(
                new org.springframework.data.mongodb.core.query.Query(
                        org.springframework.data.mongodb.core.query.Criteria.where("_id")
                                .is(vrd.getVariantId())
                                .and(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + projectIndex).exists(false)),
                new org.springframework.data.mongodb.core.query.Update()
                        .push(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).value(new ArrayList<>()),
                VariantRunData.class);

        // initialize sp run slot if missing
        mongoTemplate.updateFirst(
                new Query(
                        Criteria.where("_id")
                                .is( vrd.getVariantId())
                                .and(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + projectIndex + "." + runIndex).exists(false)),
                new Update().push(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + projectIndex).value(new ArrayList<>()),
                VariantRunData.class);

        if (newGenotypeAnnotationArray != null && !newGenotypeAnnotationArray.isEmpty()) {
            // initialize ai project slot if missing
            mongoTemplate.updateFirst(
                    new Query(
                            Criteria.where("_id")
                                    .is(vrd.getVariantId())
                                    .and(VariantRunData.SECTION_ADDITIONAL_INFO + "." + projectIndex).exists(false)),
                    new Update().push(VariantRunData.SECTION_ADDITIONAL_INFO).value(new ArrayList<>()),
                    VariantRunData.class);

            // initialize ai run slot if missing
            mongoTemplate.updateFirst(
                    new Query(
                            Criteria.where("_id")
                                    .is(vrd.getVariantId())
                                    .and(VariantRunData.SECTION_ADDITIONAL_INFO + "." + projectIndex + "." + runIndex).exists(false)),
                    new Update().push(VariantRunData.SECTION_ADDITIONAL_INFO + "." + projectIndex).value(new ArrayList<>()),
                    VariantRunData.class);
            update.set(aiPath, newGenotypeAnnotationArray);
        }

        if (newVariantRunAnnotationArray != null && !newVariantRunAnnotationArray.isEmpty()) {
            // initialize vra project slot if missing
            mongoTemplate.updateFirst(
                    new Query(
                            Criteria.where("_id")
                                    .is( vrd.getVariantId())
                                    .and(  "vra." + projectIndex).exists(false)),
                    new Update().push("vra").value(new ArrayList<>()),
                    VariantRunData.class);

            // initialize vra run slot if missing
            mongoTemplate.updateFirst(
                    new Query(
                            Criteria.where("_id")
                                    .is( vrd.getVariantId())
                                    .and("vra." + projectIndex + "." + runIndex).exists(false)),
                    new Update().push("vra." + projectIndex).value(new ArrayList<>()),
                    VariantRunData.class);
            update.set(vraPath, newVariantRunAnnotationArray);
        }

        // vai is now a single HashMap, not a nested array
        // Only overwrite if new annotation is non-null and non-empty
        if (newVariantAnnotation != null && !newVariantAnnotation.isEmpty()) {
            update.set("vai", newVariantAnnotation);
        }

        mongoTemplate.updateFirst(query, update, VariantRunData.class);
    }
    @Override
    protected void initReader(VCFParameters params) throws IOException, URISyntaxException {
        String urlString = params.getMainFileUrl().toString();
        boolean fIsRemoteFile = !urlString.startsWith("file:/");

        final FeatureCodec codec = params.isBCF() ? new RelaxedBCF2Codec() : new VCFCodec();
        boolean fIsCompressed;
        
        if (params.isBCF()) {
            InputStream testIS = params.getMainFileUrl().openStream();
            if (!testIS.markSupported())
                testIS = new BufferedInputStream(testIS);
            testIS.mark(2);
            int byte1 = testIS.read();
            int byte2 = testIS.read();
            testIS.close();
            fIsCompressed = (byte1 == 0x1f && byte2 == 0x8b);
            
            this.reader = new FlexibleBCFReader(params.getMainFileUrl(), codec, fIsCompressed);
        } else {
            fIsCompressed = urlString.toLowerCase().endsWith(".gz");
            
            if (!fIsRemoteFile || !fIsCompressed)
                this.reader = AbstractFeatureReader.getFeatureReader(urlString, codec, false);
            else {
                this.reader = (FeatureReader<VariantContext>) new TribbleIndexedFeatureReader<VariantContext, LineIterator>(urlString, codec, false) {
                    @Override
                    public CloseableTribbleIterator<VariantContext> iterator() {
                        try {
                            InputStream rawStream = new EightKBAlignedHTTPStream(params.getMainFileUrl());
                            InputStream decompressedStream = new BlockCompressedInputStream(rawStream);
                            final LineIterator lineIterator = new LineIteratorImpl(AsciiLineReader.from(decompressedStream));
                            codec.readHeader(lineIterator);
                            
                            return new CloseableTribbleIterator<VariantContext>() {
                                private final SingleLineIterator singleLineIterator = new SingleLineIterator();
                                
                                @Override
                                public boolean hasNext() { return lineIterator.hasNext(); }

                                @Override
                                public VariantContext next() {
                                    String line = lineIterator.next();
                                    if (line == null) return null;
                                    singleLineIterator.setLine(line);
                                    
                                    try {
                                        return codec.decode(singleLineIterator);
                                    } catch (IOException e) {
                                        throw new UncheckedIOException("Failed to decode variant", e);
                                    }
                                }
                                
                                @Override
                                public void remove() { throw new UnsupportedOperationException(); }
                                
                                @Override
                                public void close() {
                                    try {
                                        decompressedStream.close();
                                    } catch (IOException e) {
                                        LOG.error("Failed to close stream", e);
                                    }
                                }
                                
                                @Override
                                public java.util.Iterator<VariantContext> iterator() { return this; }

                                final class SingleLineIterator implements LineIterator {
                                    private String currentLine;
                                    private boolean consumed;
                                    
                                    public void setLine(String line) {
                                        this.currentLine = line;
                                        this.consumed = false;
                                    }
                                    
                                    @Override
                                    public boolean hasNext() {
                                        return !consumed && currentLine != null;
                                    }
                                    
                                    @Override
                                    public String next() {
                                        if (consumed || currentLine == null) {
                                            throw new NoSuchElementException();
                                        }
                                        consumed = true;
                                        return currentLine;
                                    }
                                    
                                    @Override
                                    public String peek() {
                                        return consumed ? null : currentLine;
                                    }
                                    
                                    @Override
                                    public void remove() {
                                        throw new UnsupportedOperationException();
                                    }
                                }
                            };
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to initialize custom iterator", e);
                        }
                    }
                };
            }
        }

        this.variantIterator = reader.iterator();
    }

    @Override
    protected void closeResource() throws IOException {}

    @Override
    protected Integer findPloidyLevel(MongoTemplate mongoTemplate, Integer nPloidyParam, ProgressIndicator progress) throws IOException {
        Iterator<VariantContext> variantIterator = reader.iterator();
        int nPloidy = 0, i = 0;
        while (variantIterator.hasNext() && i++ < 100 && nPloidy == 0) {
            VariantContext vcfEntry = variantIterator.next();
            if (vcfEntry.getCommonInfo().getAttribute("CNV") == null) {
                nPloidy = vcfEntry.getMaxPloidy(0);
                LOG.info("Found ploidy level of " + nPloidy + " for " + vcfEntry.getType() + " variant " + vcfEntry.getContig() + ":" + vcfEntry.getStart());
                break;
            }
        }
        return nPloidy;
    }

    private VariantRunData addVcfDataToVariant(
            MongoTemplate mongoTemplate,
            VCFHeader header,
            VariantData variantToFeed,
            Integer nAssemblyId,
            VariantContextHologram vc,
            GenotypingProject project,
            String runName,
            HashMap<String, Comparable> phasingGroup,
            int effectAnnotationPos,
            int geneIdAnnotationPos,
            HashSet<String> distinctEncounteredGeneNames) throws Exception {

        int projectIndex = project.getId();
        int runIndex = project.getRuns().indexOf(runName);
        int initialAlleleCount = variantToFeed.getKnownAlleles().size();

        // Safety check: verify the variant matches the position
//        if (nAssemblyId != null && vc != null) {
//            ReferencePosition pos = variantToFeed.getReferencePosition(nAssemblyId);
//            if (pos != null) {
//                if (!pos.getSequence().equals(vc.getContig()) || pos.getStartSite() != vc.getStart()) {
//                    throw new Exception("Variant position mismatch: DB has " +
//                        pos.getSequence() + ":" + pos.getStartSite() +
//                        ", VCF has " + vc.getContig() + ":" + vc.getStart() +
//                        " for variant " + variantToFeed.getId());
//                }
//            }
//        }

        if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType()))
            variantToFeed.setType(vc.getType().toString());
        else if (null != vc.getType() && Type.NO_VARIATION != vc.getType() && !variantToFeed.getType().equals(vc.getType().toString()))
            throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

        List<String> knownAlleleList = new ArrayList<String>();
        if (variantToFeed.getKnownAlleles().size() > 0)
            knownAlleleList.addAll(variantToFeed.getKnownAlleles());
        ArrayList<String> allelesInVC = new ArrayList<String>();
        allelesInVC.add(vc.getReference().getBaseString());
        for (Allele alt : vc.getAlternateAlleles())
            allelesInVC.add(alt.getBaseString());
        for (String vcAllele : allelesInVC)
            if (!knownAlleleList.contains(vcAllele))
                knownAlleleList.add(vcAllele);
        variantToFeed.setKnownAlleles(knownAlleleList);

        if (vc.getContig() != null && vc.getStart() != null && variantToFeed.getReferencePosition(nAssemblyId) == null)
            variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(vc.getContig(), vc.getStart(), (long) vc.getEnd()));

        VariantRunData vrd = new VariantRunData(variantToFeed.getId());


        List<String> aiEffect = new ArrayList<String>(), aiGene = new ArrayList<String>();

        // new variant additional information to be added.
        Map<String, Object> vai = vrd.getVariantAnnotation();




        List<List<HashMap<String, Object>>> variantRunAnnotationArray = new ArrayList<>();
        while (variantRunAnnotationArray.size() <= projectIndex) {
            variantRunAnnotationArray.add(new ArrayList<>());
        }
        while (variantRunAnnotationArray.get(projectIndex).size() <= runIndex) {
            variantRunAnnotationArray.get(projectIndex).add(new HashMap<>());
        }


        if (vc.isFullyDecoded())
            variantRunAnnotationArray.get(projectIndex).get(runIndex).put(VariantData.FIELD_FULLYDECODED, true);
        if (vc.hasLog10PError())
            variantRunAnnotationArray.get(projectIndex).get(runIndex).put(VariantData.FIELD_PHREDSCALEDQUAL, vc.getPhredScaledQual());
        if (!VariantData.FIELDVAL_SOURCE_MISSING.equals(vc.getSource()))
            variantRunAnnotationArray.get(projectIndex).get(runIndex).put(VariantData.FIELD_SOURCE, vc.getSource());
        if (vc.filtersWereApplied())
            variantRunAnnotationArray.get(projectIndex).get(runIndex).put(VariantData.FIELD_FILTERS, vc.getFilters().size() > 0 ? Helper.arrayToCsv(",", vc.getFilters()) : VCFConstants.PASSES_FILTERS_v4);


        Map<String, Object> attributes = vc.getAttributes();
        for (String key : attributes.keySet()) {
            if (geneIdAnnotationPos != -1 && (ANNOTATION_FIELDNAME_EFF.equals(key) || ANNOTATION_FIELDNAME_ANN.equals(key) || ANNOTATION_FIELDNAME_CSQ.equals(key))) {
                Object effectAttr = vc.getAttributes().get(key);
                List<String> effectList = effectAttr instanceof String ? Arrays.asList((String) effectAttr) : (List<String>) effectAttr;
                for (String effect : effectList) {
                    for (String effectDesc : effect.split(",")) {
                        String sEffect = null;
                        int parenthesisPos = !ANNOTATION_FIELDNAME_EFF.equals(key) ? -1 : effectDesc.indexOf("(");
                        List<String> fields = Helper.split(effectDesc.substring(parenthesisPos + 1).replaceAll("\\)", ""), "|");
                        if (parenthesisPos > 0)
                            sEffect = effectDesc.substring(0, parenthesisPos);
                        else if (effectAnnotationPos != -1)
                            sEffect = fields.get(effectAnnotationPos);
                        if (sEffect != null)
                            aiEffect.add(sEffect);
                        aiGene.add(fields.get(geneIdAnnotationPos));
                    }
                }
                vai.put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, aiGene);
                vai.put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, aiEffect);
                vai.put(ANNOTATION_FIELDNAME_ANN, effectAttr.toString());
                for (String variantEffectAnnotation : aiEffect) {
                    if (variantEffectAnnotation != null && !project.getEffectAnnotations().contains(variantEffectAnnotation)) {
                        project.getEffectAnnotations().add(variantEffectAnnotation);
                    }
                }
                if (aiGene != null)
                    distinctEncounteredGeneNames.addAll(aiGene);
            }

            else {

                Object attrVal = vc.getAttributes().get(key);
                if (attrVal instanceof ArrayList) {
                    variantRunAnnotationArray.get(projectIndex).get(runIndex).put(key, Helper.arrayToCsv(",", (ArrayList) attrVal));
                } else if (attrVal != null) {
                    if (attrVal instanceof Boolean && ((Boolean) attrVal).booleanValue()) {
                        variantRunAnnotationArray.get(projectIndex).get(runIndex).put(key, (Boolean) attrVal);
                    } else {
                        try {
                            int intVal = Integer.valueOf(attrVal.toString());
                            variantRunAnnotationArray.get(projectIndex).get(runIndex).put(key, intVal);
                        } catch (NumberFormatException nfe1) {
                            try {
                                double doubleVal = Double.valueOf(attrVal.toString());
                                variantRunAnnotationArray.get(projectIndex).get(runIndex).put(key, doubleVal);
                            } catch (NumberFormatException nfe2) {
                                variantRunAnnotationArray.get(projectIndex).get(runIndex).put(key, attrVal.toString());
                            }
                        }
                    }
                }
            }



            vrd.setVariantRunAnnotation(variantRunAnnotationArray);



        }

        // genotype fields
        boolean bVariantHasGenotypeAnnotations = false;
        Iterator<Genotype> genotypes = vc.getGenotypesOrderedByName().iterator();
        Map<String, Integer> knownAlleleStringToIndexMap = new HashMap<>();
        for (int i=0; i<knownAlleleList.size(); i++)
            knownAlleleStringToIndexMap.put(knownAlleleList.get(i), i);

        List<List<List<Integer>>> genotypeArray = new ArrayList<>();
        while (genotypeArray.size() <= projectIndex) {
            genotypeArray.add(new ArrayList<>());
        }
        while (genotypeArray.get(projectIndex).size() <= runIndex) {
            genotypeArray.get(projectIndex).add(new ArrayList<>());
        }

        List<List<List<HashMap<String, Object>>>> genotypeAnnotationArray = new ArrayList<>();
        while (genotypeAnnotationArray.size() <= projectIndex) {
            genotypeAnnotationArray.add(new ArrayList<>());
        }
        while (genotypeAnnotationArray.get(projectIndex).size() <= runIndex) {
            genotypeAnnotationArray.get(projectIndex).add(new ArrayList<>());
        }
        while (genotypes.hasNext()) {
            Genotype genotype = genotypes.next();

            boolean isPhased = genotype.isPhased();
            String sIndOrSpId = genotype.getSampleName();
            if (!genotype.isCalled()) {
//                vrd.setGenotype(project.getId(), runIndex, callsetIndex, null);
//                vrd.setAdditionalInformation(project.getId(), runIndex, callsetIndex,null);

                genotypeArray.get(projectIndex).get(runIndex).add(null);
                genotypeAnnotationArray.get(projectIndex).get(runIndex).add(null);
                continue;
            }
            Map<String, Integer> genotypeCodeCache = new HashMap<>(); // FIXME: Variable lost after the merge.

            List<String> gtAllelesAsStrings = genotype.getAlleles().stream().map(allele -> allele.getBaseString()).collect(Collectors.toList());
            int numericCode = GenotypeCodeManager.createGenotypeEncoding(gtAllelesAsStrings, knownAlleleStringToIndexMap, mongoTemplate, genotypeCodeCache);


            // vrd.setGenotype(project.getId(), runIndex, callsetIndex, numericCode);
            genotypeArray.get(projectIndex).get(runIndex).add(numericCode);

            HashMap<String,Object> additionalInformationForCallset = new HashMap<>();
            if (genotype.hasGQ()) {
                additionalInformationForCallset.put(VariantData.GT_FIELD_GQ, genotype.getGQ());
            }
            if (genotype.hasDP()) {
                additionalInformationForCallset.put(VariantData.GT_FIELD_DP, genotype.getDP());
            }

            Comparable phasedGroup = phasingGroup.get(sIndOrSpId);
            if (phasedGroup == null || (!isPhased && !genotype.isNoCall()))
                phasingGroup.put(sIndOrSpId, variantToFeed.getId());
            if (isPhased) {
                bVariantHasGenotypeAnnotations = true;
                additionalInformationForCallset.put(VariantData.GT_FIELD_PHASED_GT, VariantData.rebuildVcfFormatGenotype(knownAlleleStringToIndexMap, gtAllelesAsStrings, isPhased, true));
                additionalInformationForCallset.put(VariantData.GT_FIELD_PHASED_ID, phasingGroup.get(sIndOrSpId));
            }
            boolean fSkipPlFix = false;
            if (genotype.hasAD()) {
                int[] adArray = genotype.getAD(), originalAdArray = adArray;
                adArray = VariantData.fixAdFieldValue(adArray, vc.getAlleles(), knownAlleleList);
                if (originalAdArray == adArray)
                    fSkipPlFix = true;  // if AD was correct then PL is too
                bVariantHasGenotypeAnnotations = true;
                additionalInformationForCallset.put(VariantData.GT_FIELD_AD, Helper.arrayToCsv(",", adArray));
            }
            if (genotype.hasPL()) {
                int[] plArray = genotype.getPL();
                if (!fSkipPlFix)
                    plArray = VariantData.fixPlFieldValue(plArray, genotype.getPloidy(), vc.getAlleles(), knownAlleleList);
                bVariantHasGenotypeAnnotations = true;
                additionalInformationForCallset.put(VariantData.GT_FIELD_PL, Helper.arrayToCsv(",", plArray));
            }
            Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
            for (String sAttrName : extendedAttributes.keySet()) {
                VCFFormatHeaderLine formatHeaderLine = header.getFormatHeaderLine(sAttrName);
                if (formatHeaderLine != null) {
                    boolean fConvertToNumber = (formatHeaderLine.getType().equals(VCFHeaderLineType.Integer) || formatHeaderLine.getType().equals(VCFHeaderLineType.Float)) && formatHeaderLine.isFixedCount() && formatHeaderLine.getCount() == 1;
                    String value = extendedAttributes.get(sAttrName).toString();
                    Object correctlyTypedValue = fConvertToNumber ? Float.parseFloat(value) : value;
                    if (fConvertToNumber && !formatHeaderLine.getType().equals(VCFHeaderLineType.Float))
                        correctlyTypedValue = Math.round((float) correctlyTypedValue);
                    bVariantHasGenotypeAnnotations = true;
                    additionalInformationForCallset.put(sAttrName, correctlyTypedValue);
                }
            }

            if (genotype.isFiltered()) {
                bVariantHasGenotypeAnnotations = true;
                additionalInformationForCallset.put(VariantData.FIELD_FILTERS, genotype.getFilters());
            }
            // always add an entry (null when there is nothing to store) so this list stays aligned with genotypeArray
            if (!additionalInformationForCallset.isEmpty()) {
                bVariantHasGenotypeAnnotations = true;
                genotypeAnnotationArray.get(projectIndex).get(runIndex).add(additionalInformationForCallset);
            }
            else
                genotypeAnnotationArray.get(projectIndex).get(runIndex).add(null);
//            Comparable phasedGroup = phasingGroup.get(sIndOrSpId);
//            if (phasedGroup == null || (!isPhased && !genotype.isNoCall()))
//                phasingGroup.put(sIndOrSpId, variantToFeed.getId());
//
//            List<String> gtAllelesAsStrings = genotype.getAlleles().stream().map(allele -> allele.getBaseString()).collect(Collectors.toList());
//
//            String gtCode = VariantData.rebuildVcfFormatGenotype(knownAlleleStringToIndexMap, gtAllelesAsStrings, isPhased, false);
//            if ("1/0".equals(gtCode))
//                gtCode = "0/1"; // convert to "0/1" so that MAF queries can work reliably
//
//            SampleGenotype aGT = new SampleGenotype(gtCode);
//            if (isPhased) {
//                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_GT, VariantData.rebuildVcfFormatGenotype(knownAlleleStringToIndexMap, gtAllelesAsStrings, isPhased, true));
//                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_ID, phasingGroup.get(sIndOrSpId));
//            }
//            if (genotype.hasGQ()) {
//                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_GQ, genotype.getGQ());
//            }
//            if (genotype.hasDP()) {
//                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_DP, genotype.getDP());
//            }
//            boolean fSkipPlFix = false; // for performance
//            if (genotype.hasAD()) {
//                int[] adArray = genotype.getAD(), originalAdArray = adArray;
//                adArray = VariantData.fixAdFieldValue(adArray, vc.getAlleles(), knownAlleleList);
//                if (originalAdArray == adArray)
//                    fSkipPlFix = true;  // if AD was correct then PL is too
//                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_AD, Helper.arrayToCsv(",", adArray));
//            }
//            if (genotype.hasPL()) {
//                int[] plArray = genotype.getPL();
//                if (!fSkipPlFix)
//                    plArray = VariantData.fixPlFieldValue(plArray, genotype.getPloidy(), vc.getAlleles(), knownAlleleList);
//                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PL, Helper.arrayToCsv(",", plArray));
//            }
//            Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
//            for (String sAttrName : extendedAttributes.keySet()) {
//                VCFFormatHeaderLine formatHeaderLine = header.getFormatHeaderLine(sAttrName);
//                if (formatHeaderLine != null) {
//                    boolean fConvertToNumber = (formatHeaderLine.getType().equals(VCFHeaderLineType.Integer) || formatHeaderLine.getType().equals(VCFHeaderLineType.Float)) && formatHeaderLine.isFixedCount() && formatHeaderLine.getCount() == 1;
//                    String value = extendedAttributes.get(sAttrName).toString();
//                    Object correctlyTypedValue = fConvertToNumber ? Float.parseFloat(value) : value;
//                    if (fConvertToNumber && !formatHeaderLine.getType().equals(VCFHeaderLineType.Float))
//                        correctlyTypedValue = Math.round((float) correctlyTypedValue);
//                    aGT.getAdditionalInfo().put(sAttrName, correctlyTypedValue);
//                }
//            }
//
//            if (genotype.isFiltered())
//                aGT.getAdditionalInfo().put(VariantData.FIELD_FILTERS, genotype.getFilters());
//
//            if (genotype.isCalled() || !aGT.getAdditionalInfo().isEmpty())  // otherwise there's no point in persisting an empty object
//            	vrd.getSampleGenotypes().put(m_providedIdToCallsetMap.get(sIndOrSpId).getId(), aGT);
        }

        if (project.getId() > 1 || project.getRuns().size() > 0)
            updateExistingVrdAlleles(mongoTemplate, initialAlleleCount, variantToFeed);

        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setPositions(variantToFeed.getPositions());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        vrd.setGenotypeArray(genotypeArray);
        if (bVariantHasGenotypeAnnotations) vrd.setGenotypeAnnotationArray(genotypeAnnotationArray);
        variantToFeed.setVariantAnnotation(vrd.getVariantAnnotation());
        return vrd;
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

    static public class VariantContextHologram {
        private Type type;
        private List<Allele> alleles;
        private Iterable<Genotype> genotypesOrderedByName;
        private Map<String, Object> attributes;
        private boolean filtersWereApplied;
        private Set<String> filters;
        private double phredScaledQual;
        private String source;
        private boolean hasLog10PError;
        private boolean isFullyDecoded;
        private Integer start;
        private Integer end;
        private String contig;
        private List<Allele> alternateAlleles;
        private Allele reference;
        private boolean hasID;
        private String id;
        private boolean isVariant;

        public VariantContextHologram(VariantContext vc) {
            hasID = vc.hasID();
            id = vc.getID();
            isVariant = vc.isVariant();
            type = vc.getType();
            alleles = vc.getAlleles();
            genotypesOrderedByName = vc.getGenotypesOrderedByName();
            attributes = vc.getAttributes();
            filtersWereApplied = vc.filtersWereApplied();
            filters = vc.getFilters();
            phredScaledQual = vc.getPhredScaledQual();
            source = vc.getSource();
            hasLog10PError = vc.hasLog10PError();
            isFullyDecoded = vc.isFullyDecoded();
            start = vc.getStart();
            end = vc.getEnd();
            contig = vc.getContig();
            alternateAlleles = vc.getAlternateAlleles();
            reference = vc.getReference();
        }

        public boolean hasID() { return hasID; }
        public boolean isVariant() { return isVariant; }
        public String getID() { return id; }
        public Type getType() { return type; }
        public List<Allele> getAlleles() { return alleles; }
        public Iterable<Genotype> getGenotypesOrderedByName() { return genotypesOrderedByName; }
        public Map<String, Object> getAttributes() { return attributes; }
        public boolean filtersWereApplied() { return filtersWereApplied; }
        public Set<String> getFilters() { return filters; }
        public double getPhredScaledQual() { return phredScaledQual; }
        public String getSource() { return source; }
        public boolean hasLog10PError() { return hasLog10PError; }
        public boolean isFullyDecoded() { return isFullyDecoded; }
        public Integer getStart() { return start; }
        public Integer getEnd() { return end; }
        public String getContig() { return contig; }
        public List<Allele> getAlternateAlleles() { return alternateAlleles; }
        public Allele getReference() { return reference; }

        public void setContig(String contig) {
            this.contig = contig;
        }

        public void setStart(Integer start) {
            this.start = start;
        }
    }
}