package fr.cirad.mgdb.importing;

import java.io.*;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapCodec;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapFeature;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
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
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class DartImport extends AbstractGenotypeImport {

    /** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);

	/** The m_process id. */
	private String m_processID;

	private int nNumProc = Runtime.getRuntime().availableProcessors();

    public DartImport(){

    }

    /**
     * Instantiates a new dart import.
     *
     * @param processID the process id
     */
    public DartImport(String processID)
    {
        m_processID = processID;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, HapMap file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

        File mainFile = new File(args[4]);
        if (!mainFile.exists() || mainFile.length() == 0)
            throw new Exception("File " + args[4] + " is missing or empty!");

        int mode = 0;
        try
        {
            mode = Integer.parseInt(args[5]);
        }
        catch (Exception e)
        {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        new DartImport().importToMongo(args[0], args[1], args[2], args[3], null, new File(args[4]).toURI().toURL(), args[5], null, false, mode);
    }


    public List<DartInfo> getDartInfo(String path) throws Exception {
        FileReader fileReader = new FileReader(path);

        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String individualName = null;
        String alleleID;
        String cloneID;
        String alleleSequence;
        String trimmedSequence;
        String chrom;
        Integer chromPos;
        String alnCnt;
        String alnEvalue;

        List<DartInfo> result = new ArrayList<DartInfo>();

        String line = bufferedReader.readLine();
        while (line != null) {
            while (line != null && line.startsWith("*,")) {
                line = bufferedReader.readLine();
            }

            if (line == null) {
                throw new Exception("This file is not a valid Dart format file.");
            }

            if (line.startsWith("AlleleID")) {
                String[] columnName = line.split(",");
                 individualName = columnName[5].substring(columnName[5].indexOf('_') + 1);
            }
            else {
                String[] columns = line.split(",");
                alleleID = columns[0];
                cloneID = columns[1];
                alleleSequence = columns[2];
                trimmedSequence = columns[3];
                chrom = columns[4];
                chromPos =  Integer.parseInt(columns[5]);
                alnCnt = columns[6];
                alnEvalue = columns[7];

                result.add(new DartInfo(alleleID, cloneID, alleleSequence, trimmedSequence, chrom, chromPos, alnCnt, alnEvalue, individualName));
            }


            line = bufferedReader.readLine();
        }
        return result;
    }


    public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, URL mainFileUrl, String assemblyName, HashMap<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode) throws Exception
    {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
        progress.setPercentageEnabled(false);

        Integer createdProject = null;

        FeatureReader<RawHapMapFeature> reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), new RawHapMapCodec(), false);
        GenericXmlApplicationContext ctx = null;
        try
        {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null)
            {	// we are probably being invoked offline
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

            if (m_processID == null)
                m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && nPloidy != null && project.getPloidyLevel() != nPloidy)
                throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nPloidy + ") data!");

            MongoTemplateManager.lockProjectForWriting(sModule, sProject);

            cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

            if (project == null || importMode > 0) {	// create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
//				project.setOrigin(2 /* Sequencing */);
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
                if (importMode != 1)
                    createdProject = project.getId();
            }

            progress.addStep("Scanning existing marker IDs");
            progress.moveToNextStep();
            Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

            String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
            AtomicInteger nNumberOfVariantsToSaveAtOnce = new AtomicInteger(1), totalProcessedVariantCount = new AtomicInteger(0);
            final ArrayList<String> sampleIds = new ArrayList<>();
            progress.addStep("Processing variant lines");
            progress.moveToNextStep();

            int nNConcurrentThreads = Math.max(1, nNumProc);
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nNConcurrentThreads + " threads");

            Iterator<RawHapMapFeature> it = reader.iterator();
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
            final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
            if (assemblyIDs.isEmpty())
                assemblyIDs.add(null);	// old-style, assembly-less DB

            int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
            Thread[] importThreads = new Thread[nImportThreads];
            boolean fDbAlreadyContainedVariants = mongoTemplate.findOne(new Query() {{ fields().include("_id"); }}, VariantData.class) != null;

            final GenotypingProject finalProject = project;

            final MongoTemplate finalMongoTemplate = mongoTemplate;
            final Assembly finalAssembly = assembly;
            m_providedIdToSampleMap = new HashMap<String /*individual*/, GenotypingSample>();

            for (int threadIndex = 0; threadIndex < nImportThreads; threadIndex++) {
                importThreads[threadIndex] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            int numberOfVariantsProcessedInThread = 0, localNumberOfVariantsToSaveAtOnce = -1;  // Minor optimization, copy this locally to avoid having to use the AtomicInteger every time
                            HashSet<VariantData> unsavedVariants = new HashSet<VariantData>();  // HashSet allows no duplicates
                            HashSet<VariantRunData> unsavedRuns = new HashSet<VariantRunData>();
                            while (progress.getError() == null && !progress.isAborted()) {
                                RawHapMapFeature hmFeature = null;

                                synchronized(it) {
                                    if (it.hasNext())
                                        hmFeature = it.next();
                                }
                                if (hmFeature == null)
                                    break;

                                // We can only retrieve the sample IDs from a feature but need to set them up synchronously
                                if (numberOfVariantsProcessedInThread == 0) {
                                    synchronized (sampleIds) {
                                        // The first thread to reach this will create the samples, the next ones will skip
                                        // So this will be executed once before everything else, everything after this block of code can assume the samples have been set up
                                        if (sampleIds.isEmpty()) {
                                            sampleIds.addAll(Arrays.asList(hmFeature.getSampleIDs()));

                                            HashSet<Individual> indsToAdd = new HashSet<>();
                                            boolean fDbAlreadyContainedIndividuals = finalMongoTemplate.findOne(new Query(), Individual.class) != null;
                                            for (String sIndOrSpId : sampleIds) {
                                                String sIndividual = sampleToIndividualMap == null ? sIndOrSpId : sampleToIndividualMap.get(sIndOrSpId);
                                                if (sIndividual == null) {
                                                    progress.setError("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
                                                    return;
                                                }

                                                if (!fDbAlreadyContainedIndividuals || finalMongoTemplate.findById(sIndividual, Individual.class) == null)  // we don't have any population data so we don't need to update the Individual if it already exists
                                                    indsToAdd.add(new Individual(sIndividual));

                                                if (!indsToAdd.isEmpty() && indsToAdd.size() % 1000 == 0) {
                                                    finalMongoTemplate.insert(indsToAdd, Individual.class);
                                                    indsToAdd = new HashSet<>();
                                                }

                                                int sampleId = AutoIncrementCounter.getNextSequence(finalMongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                                                m_providedIdToSampleMap.put(sIndOrSpId, new GenotypingSample(sampleId, finalProject.getId(), sRun, sIndividual, sampleToIndividualMap == null ? null : sIndOrSpId));   // add a sample for this individual to the project
                                            }

                                            finalMongoTemplate.insert(m_providedIdToSampleMap.values(), GenotypingSample.class);
                                            if (!indsToAdd.isEmpty()) {
                                                finalMongoTemplate.insert(indsToAdd, Individual.class);
                                                indsToAdd = null;
                                            }
                                            m_fSamplesPersisted = true;
                                            nNumberOfVariantsToSaveAtOnce.set(sampleIds.size() == 0 ? nMaxChunkSize : Math.max(1, nMaxChunkSize / sampleIds.size()));
                                            LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce.get());
                                        }
                                    }

                                    localNumberOfVariantsToSaveAtOnce = nNumberOfVariantsToSaveAtOnce.get();
                                }

                                try
                                {
                                    Type variantType = determineType(Arrays.stream(hmFeature.getAlleles()).map(allele -> Allele.create(allele)).collect(Collectors.toList()));
                                    String sFeatureName = hmFeature.getName().trim();
                                    boolean fFileProvidesValidVariantId = !sFeatureName.isEmpty() && !".".equals(sFeatureName);

                                    String variantId = null;
                                    for (String variantDescForPos : getIdentificationStrings(variantType.toString(), hmFeature.getChr(), (long) hmFeature.getStart(), sFeatureName.length() == 0 ? null : Arrays.asList(new String[] {sFeatureName}))) {
                                        variantId = existingVariantIDs.get(variantDescForPos);
                                        if (variantId != null)
                                            break;
                                    }

                                    if (variantId == null && fSkipMonomorphic) {
                                        String[] distinctGTs = Arrays.stream(hmFeature.getGenotypes()).filter(gt -> !"NA".equals(gt) && !"NN".equals(gt)).distinct().toArray(String[]::new);
                                        if (distinctGTs.length == 0 || (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split(variantType.equals(Type.SNP) ? "" : "/")).distinct().count() < 2))
                                            continue; // skip non-variant positions that are not already known
                                    }

                                    VariantData variant = variantId == null || !fDbAlreadyContainedVariants ? null : finalMongoTemplate.findById(variantId, VariantData.class);
                                    if (variant == null) {
                                        if (fFileProvidesValidVariantId) {
                                            variant = new VariantData((ObjectId.isValid(sFeatureName) ? "_" : "") + sFeatureName);
                                            totalProcessedVariantCount.getAndIncrement();
                                        }
                                        else
                                            variant = new VariantData(generatedIdBaseString + String.format(String.format("%09x", totalProcessedVariantCount.getAndIncrement())));
                                    }
                                    else
                                        totalProcessedVariantCount.getAndIncrement();

                                    //update variant runs
                                    variant.getRuns().add(new Run(finalProject.getId(), sRun));

                                    AtomicInteger allIdx = new AtomicInteger(0);
                                    Map<String, Integer> alleleIndexMap = variant.getKnownAlleles().stream().collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));  // should be more efficient not to call indexOf too often...
                                    List<Allele> knownAlleles = new ArrayList<>();
                                    for (String allele : hmFeature.getAlleles()) {
                                        if (!alleleIndexMap.containsKey(allele)) {  // it's a new allele
                                            int alleleIndexMapSize = alleleIndexMap.size();
                                            alleleIndexMap.put(allele, alleleIndexMapSize);
                                            variant.getKnownAlleles().add(allele);
                                            knownAlleles.add(Allele.create(allele, alleleIndexMapSize == 0));
                                        }
                                    }

//                                    VariantRunData runToSave = addHapMapDataToVariant(finalMongoTemplate, variant, finalAssembly == null ? null : finalAssembly.getId(), variantType, alleleIndexMap, hmFeature, finalProject, sRun, sampleIds);

                                    for (Integer asmId : assemblyIDs) {
                                        ReferencePosition rp = variant.getReferencePosition(asmId);
                                        if (rp != null)
                                            finalProject.getContigs(asmId).add(rp.getSequence());
                                    }

                                    finalProject.getAlleleCounts().add(variant.getKnownAlleles().size());	// it's a TreeSet so it will only be added if it's not already present

                                    if (variant.getKnownAlleles().size() > 0)
                                    {	// we only import data related to a variant if we know its alleles
                                        if (!unsavedVariants.contains(variant))
                                            unsavedVariants.add(variant);
//                                        if (!unsavedRuns.contains(runToSave))
//                                            unsavedRuns.add(runToSave);
                                    }

                                    numberOfVariantsProcessedInThread++;
                                    int currentTotalProcessedVariants = totalProcessedVariantCount.get();
                                    if (currentTotalProcessedVariants % localNumberOfVariantsToSaveAtOnce == 0) {
                                        saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, finalMongoTemplate, progress, saveService);
                                        unsavedVariants = new HashSet<>();
                                        unsavedRuns = new HashSet<>();
                                    }

                                    progress.setCurrentStepProgress(currentTotalProcessedVariants);
                                    if (currentTotalProcessedVariants % (localNumberOfVariantsToSaveAtOnce * 50) == 0)
                                        LOG.debug(currentTotalProcessedVariants + " lines processed");
                                }
                                catch (Exception e)
                                {
                                    LOG.error("Error occured importing variant number " + (totalProcessedVariantCount.get() + 1) + " (" + Type.SNP.toString() + ":" + hmFeature.getChr() + ":" + hmFeature.getStart() + ") ", e);
                                    throw new Exception("Error occured importing variant number " + (totalProcessedVariantCount.get() + 1) + " (" + Type.SNP.toString() + ":" + hmFeature.getChr() + ":" + hmFeature.getStart() + ") " + (e.getMessage().endsWith("\"index\" is null") ? "containing an invalid allele code" : e.getMessage()), e);
                                }
                            }
                            if (unsavedVariants.size() > 0) {
                                persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), finalMongoTemplate, unsavedVariants, unsavedRuns);
                                progress.setCurrentStepProgress(totalProcessedVariantCount.get());
                            }
                        } catch (Throwable t) {
                            progress.setError("Genotype import failed with error: " + t.getMessage());
                            LOG.error(progress.getError(), t);
                            return;
                        }
                    }
                };

                importThreads[threadIndex].start();
            }

            for (int i = 0; i < nImportThreads; i++)
                importThreads[i].join();

            reader.close();

            saveService.shutdown();
            saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

            if (progress.getError() != null || progress.isAborted())
                return createdProject;

            // save project data
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);

            LOG.info("HapMapImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + totalProcessedVariantCount.get() + " records");

            return createdProject;
        }
        catch (Exception e) {
            LOG.error("Error", e);
            progress.setError(e.getMessage());
            return createdProject;
        }
        finally
        {
            if (m_fCloseContextAfterImport && ctx != null)
                ctx.close();

            reader.close();

            MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
            if (progress.getError() == null && !progress.isAborted()) {
                progress.addStep("Preparing database for searches");
                progress.moveToNextStep();
                MgdbDao.prepareDatabaseForSearches(sModule);
            }
        }
    }
}