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

    /** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);

	/** The m_process id. */
	//private String m_processID;

    private DartIterator dartIterator;

	private int nNumProc = Runtime.getRuntime().availableProcessors();

    public DartImport(){}

    /**
     * Instantiates a new dart import.
     *
     * @param processID the process id
     */
    public DartImport(String processID)
    {
        m_processID = processID;
    }



    static final class DartIterator implements Iterator<List<DartInfo>> {
        
    	Boolean twoRow = null;
//        int lineIndex = 0;
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
//                lineIndex++;
                String alleleID2 = line.split(",")[0].split("[^0-9]")[0];
                twoRow = alleleID1.equals(alleleID2);
                return genericDartLine(line1, line, scanner, fieldPositions/*, individualName*/, twoRow, columnNames);
            }
            else {
                return genericDartLine(line, null, scanner, fieldPositions/*, individualName*/, twoRow, columnNames);
//                if (twoRow)
//                    lineIndex++;
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

//        List<DartInfo> result = new ArrayList<DartInfo>();

//        String individualName = null;
//        try {

	
	        String line = scanner.nextLine();


	        while (scanner.hasNextLine() && line.startsWith("*,"))
	            line = scanner.nextLine();
	
	        if (line == null)
	            throw new Exception("This file is not in a valid Dart format");
	

//	        while (scanner.hasNextLine()) {
//	            if (lineIndex == 0) {
	                columnNames = line.split(",");
	                colLoop: for (int i = 0; i < columnNames.length; i++) {
	                	for (String mandatoryColPrefix : new String[] {"Chrom_", "ChromPos_"})
	                		if (columnNames[i].startsWith(mandatoryColPrefix)) {
//	                        individualName = columnName[i].substring(columnName[i].indexOf('_') + 1);
	                			fieldPositions.put(mandatoryColPrefix + "*", i);
	                			continue colLoop;
	                		}
	                    fieldPositions.put(columnNames[i], i);
	                };
	                for (String mandatoryCol : new String[] {"AlleleID", "AlleleSequence", "TrimmedSequence", "SnpPosition", /*"strand", */ "CallRate", "FreqHomRef", "FreqHomSnp", "FreqHets", "RepAvg", "Chrom_*", "ChromPos_*"})
		                if (fieldPositions.get(mandatoryCol) == null)
		                	throw new Exception("Unable to find mandatory field '" + mandatoryCol + "' in Dart file header!");
//	            }
//	            else {}
//	            line = scanner.nextLine();
//	            lineIndex++;
//	        }
//        }
//        finally {
//        	scanner.close();
//        }

    	DartIterator dartIterator = new DartIterator(scanner, columnNames, fieldPositions);
        return dartIterator;
    }

    private static List<DartInfo> genericDartLine (String line, String startLine, Scanner scanner, HashMap<String, Integer> fieldPositions/*, String individualName*/, boolean tworow, String[] columnNames) {
    	List<DartInfo> result = new ArrayList<>();
        String[] columns = line.split(",");
        
        if (columns.length != columnNames.length)
        	throw new Error("Line has " + columns.length + " fields instead of " + columnNames.length + ": " + line.split(",")[0]);
        
        String alleleID = columns[fieldPositions.get("AlleleID")];
        DartInfo dart = new DartInfo(alleleID/*, individualName*/);
        dart.setAlleleSequence(columns[fieldPositions.get("AlleleSequence")]);
        dart.setTrimmedSequence(columns[fieldPositions.get("TrimmedSequence")]);
        dart.setChrom(columns[fieldPositions.get("Chrom_*")]);
        dart.setChromPos(Integer.parseInt(columns[fieldPositions.get("ChromPos_*")]));
        dart.setSnpPos(Integer.parseInt(columns[fieldPositions.get("SnpPosition")]));
//        dart.setStrand(columns[columnNames.get("strand")]);
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
                result.addAll(genericDartLine(startLine, null, scanner, fieldPositions, /*individualName, */tworow, columnNames));
                return result;
            }

        }
        else {
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
            case 0:
                return "" + ref + ref;
            case 1:
                return "" + alt + alt;
            case 2:
                return "" + ref + alt;
            case 3:
                return "NN";
            default:
                throw new Error("Sample's state have to be 0, 1 or 2 on OneRow");
        }
    }

    public static String genotypeOfSampleTwoRow(char ref, char alt, int state1, int state2) {
        switch (state1) {
            case 0:
                if (state2 == 0)
                    return "" + ref + ref;
                if (state2 == 1)
                    return "" + ref + alt;
                throw new Error("Sample's state have to be 0 or 1 on TwoRow");
            case 1:
                if (state2 == 0)
                    return "" + alt + ref;
                if (state2 == 1)
                    return "" + alt + alt;
                throw new Error("Sample's state have to be 0 or 1 on TwoRow");
            case 3:
                return "NN";
            default:
                throw new Error("Sample's state have to be 0 or 1 on TwoRow");
        }
    }


    @Override
    protected long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sModule = params.getModule();
        String sProject = params.getRun();
        String sRun = params.getRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();

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

//            Iterator<DartInfo> it = reader.iterator();
        DartIterator dataReader = getDartInfo(params.getMainFileUrl());
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

        VCFInfoHeaderLine headerLineGT = new VCFInfoHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
        VCFInfoHeaderLine headerLineAS = new VCFInfoHeaderLine("AS", 2, VCFHeaderLineType.String, "AlleleSequence - In 1 row format: the sequence of the Reference allele. In 2 rows format: the sequence of the Reference allele is in the Ref row, the sequence of the SNP allele in the SNP row");
        VCFInfoHeaderLine headerLineSP = new VCFInfoHeaderLine("SP", 3, VCFHeaderLineType.Integer, "SnpPosition - The position (zero indexed) in the sequence tag at which the defined SNP variant base occurs");
        VCFInfoHeaderLine headerLineCR = new VCFInfoHeaderLine("CR", 4, VCFHeaderLineType.Float, "CallRate - The proportion of samples for which the genotype call is either '1' or '0', rather than '-'");
        VCFInfoHeaderLine headerLineFHR = new VCFInfoHeaderLine("FHR", 5, VCFHeaderLineType.Float, "FreqHomRef - The proportion of samples which score as homozygous for the Reference allele");
        VCFInfoHeaderLine headerLineFHS = new VCFInfoHeaderLine("FHS", 6, VCFHeaderLineType.Float, "FreqHomSnp - The proportion of samples which score as homozygous for the SNP allele");
        VCFInfoHeaderLine headerLineFH = new VCFInfoHeaderLine("FH", 7, VCFHeaderLineType.Float, "FreqHet - The proportion of samples which score as heterozygous");

        VCFHeader header = new VCFHeader(new HashSet<>(Arrays.asList(headerLineGT, headerLineAS, headerLineSP, headerLineCR, headerLineFHR, headerLineFHS, headerLineFH)));
        finalMongoTemplate.save(new DBVCFHeader(new DBVCFHeader.VcfHeaderId(finalProject.getId(), sRun), header));

        for (int threadIndex = 0; threadIndex < nImportThreads; threadIndex++) {
            importThreads[threadIndex] = new Thread() {
                @Override
                public void run() {
                    try {
                        int numberOfVariantsProcessedInThread = 0, localNumberOfVariantsToSaveAtOnce = -1;  // Minor optimization, copy this locally to avoid having to use the AtomicInteger every time
                        HashSet<VariantData> unsavedVariants = new HashSet<VariantData>();  // HashSet allows no duplicates
                        HashSet<VariantRunData> unsavedRuns = new HashSet<VariantRunData>();
                        while (progress.getError() == null && !progress.isAborted()) {
                            List<DartInfo> dartFeatures = null;
                            synchronized(dataReader) {
                                if (dataReader.hasNext())
                                    dartFeatures= dataReader.next();
                            }
                            if (dartFeatures == null)
                                break;

                            // We can only retrieve the sample IDs from a feature but need to set them up synchronously
                            if (numberOfVariantsProcessedInThread == 0) {
                                synchronized (sampleIds) {
                                    // The first thread to reach this will create the samples, the next ones will skip
                                    // So this will be executed once before everything else, everything after this block of code can assume the samples have been set up
                                    if (sampleIds.isEmpty()) {
                                        sampleIds.addAll(Arrays.asList(dartFeatures.iterator().next().getSampleIDs()));

                                        createCallSetsSamplesIndividuals(sampleIds, finalMongoTemplate, finalProject.getId(), sRun, sampleToIndividualMap, progress);
                                        setSamplesPersisted(true);

                                        nNumberOfVariantsToSaveAtOnce.set(sampleIds.size() == 0 ? nMaxChunkSize : Math.max(1, nMaxChunkSize / sampleIds.size()));
                                        LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce.get());
                                    }
                                }

                                localNumberOfVariantsToSaveAtOnce = nNumberOfVariantsToSaveAtOnce.get();
                            }

                            for (DartInfo dartFeature : dartFeatures)
                                try {
                                    Type variantType = determineType(Arrays.stream(dartFeature.getAlleles()).map(allele -> Allele.create(allele)).collect(Collectors.toList()));
                                    String sFeatureName = dartFeature.getAlleleID().trim(); //getName()
                                    boolean fFileProvidesValidVariantId = !sFeatureName.isEmpty() && !".".equals(sFeatureName);

                                    String variantId = null;
                                    for (String variantDescForPos : getIdentificationStrings(variantType.toString(), dartFeature.getChrom(), (long) dartFeature.getStart(), sFeatureName.length() == 0 ? null : Arrays.asList(new String[] {sFeatureName}))) {
                                        variantId = existingVariantIDs.get(variantDescForPos);
                                        if (variantId != null)
                                            break;
                                    }

                                    if (variantId == null && fSkipMonomorphic) {
                                        String[] distinctGTs = Arrays.stream(dartFeature.getGenotypes()).filter(gt -> !"NA".equals(gt) && !"NN".equals(gt)).distinct().toArray(String[]::new);
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

                                    // update variant runs
                                    variant.getRuns().add(new Run(finalProject.getId(), sRun));

                                    int initialAlleleCount = variant.getKnownAlleles().size();
                                    AtomicInteger allIdx = new AtomicInteger(0);
                                    Map<String, Integer> alleleIndexMap = variant.getKnownAlleles().stream().collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));  // should be more efficient not to call indexOf too often...
                                    List<Allele> knownAlleles = new ArrayList<>();
                                    for (String allele : dartFeature.getAlleles()) {
                                        if (!alleleIndexMap.containsKey(allele)) {  // it's a new allele
                                            int alleleIndexMapSize = alleleIndexMap.size();
                                            alleleIndexMap.put(allele, alleleIndexMapSize);
                                            variant.getKnownAlleles().add(allele);
                                            knownAlleles.add(Allele.create(allele, alleleIndexMapSize == 0));
                                        }
                                    }

                                    VariantRunData runToSave = addDartSeqDataToVariant(finalMongoTemplate, variant, finalAssembly == null ? null : finalAssembly.getId(), variantType, alleleIndexMap, dartFeature, finalProject, sRun, sampleIds, initialAlleleCount);

                                    runToSave.getAdditionalInfo().put("AS", dartFeature.getAlleleSequence());
                                    runToSave.getAdditionalInfo().put("SP", dartFeature.getSnpPos());
                                    runToSave.getAdditionalInfo().put("CR", dartFeature.getCallRate());
                                    runToSave.getAdditionalInfo().put("FHR", dartFeature.getFreqHomRef());
                                    runToSave.getAdditionalInfo().put("FHS", dartFeature.getFreqHomSnp());
                                    runToSave.getAdditionalInfo().put("FH", dartFeature.getFreqHets());

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
                                        if (!unsavedRuns.contains(runToSave))
                                            unsavedRuns.add(runToSave);
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
                                catch (Exception e) {
                                    LOG.error("Error occured importing variant number " + (totalProcessedVariantCount.get() + 1) + " (" + Type.SNP.toString() + ":" + dartFeature.getChrom() + ":" + dartFeature.getStart() + ") ", e);
                                    throw new Exception("Error occured importing variant number " + (totalProcessedVariantCount.get() + 1) + " (" + Type.SNP.toString() + ":" + dartFeature.getChrom() + ":" + dartFeature.getStart() + ") " + (e.getMessage().endsWith("\"index\" is null") ? "containing an invalid allele code" : e.getMessage()), e);
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


        saveService.shutdown();
        saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

        if (progress.getError() != null || progress.isAborted())
            return 0;

        return totalProcessedVariantCount.get();
        //LOG.info("DartSeqImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + totalProcessedVariantCount.get() + " records");

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

    /**
     * Adds the dartseq data to variant.
     *
     * @param mongoTemplate the mongo template
     * @param variantToFeed the variant to feed
     * @param nAssemblyId the assembly id
     * @param variantType variant type
     * @param alleleIndexMap map providing the numeric index for each allele
     * @param dartFeature the hm feature
     * @param project the project
     * @param runName the run name
     * @param individuals the individuals
     * @param initialAlleleCount
     * @return the variant run data
     * @throws Exception the exception
     */
    private VariantRunData addDartSeqDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId, Type variantType, Map<String, Integer> alleleIndexMap, DartInfo dartFeature, GenotypingProject project, String runName, List<String>individuals, int initialAlleleCount) throws Exception {
        boolean fSNP = variantType.equals(Type.SNP);

		if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType()))
			variantToFeed.setType(variantType.toString());
		else if (null != variantType && Type.NO_VARIATION != variantType && !variantToFeed.getType().equals(variantType.toString()))
			throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

        if (variantToFeed.getReferencePosition(nAssemblyId) == null)    // otherwise we leave it as it is (had some trouble with overridden end-sites)
            variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(dartFeature.getChrom(), dartFeature.getStart(), (long) dartFeature.getEnd()));

		// take into account ref and alt alleles (if it's not too late)
		if (variantToFeed.getKnownAlleles().size() == 0)
			variantToFeed.setKnownAlleles(Arrays.stream(dartFeature.getAlleles()).collect(Collectors.toList()));

		VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
		HashSet<Integer> ploidiesFound = new HashSet<>();
        String[] genotypes = dartFeature.getGenotypes();
		for (int i=0; i<genotypes.length; i++) {
            String genotype = genotypes[i].toUpperCase();
            if (genotype.startsWith("N"))
                continue;    // we don't add missing genotypes

//            if (genotype.length() == 1) {
//                String gtForIupacCode = iupacCodeConversionMap.get(genotype);
//                if (gtForIupacCode != null)
//                    genotype = gtForIupacCode;    // it's a IUPAC code, let's convert it to a pair of bases
//            }

            List<String> alleles = null;
            if (genotype.contains("/")) {
                alleles = Helper.split(genotype, "/");
                ploidiesFound.add(alleles.size());
            }
            else if (alleleIndexMap.containsKey(genotype))
                alleles = Collections.nCopies(project.getPloidyLevel(), genotype);    // must be a collapsed homozygous
            else if (fSNP)
                alleles = Arrays.asList(genotype.split(""));

            String sIndOrSpId = individuals.get(i);
            if (alleles == null || alleles.isEmpty()) {
                LOG.warn("Ignoring invalid genotype \"" + genotype + "\" for variant " + variantToFeed.getId() + " and individual " + sIndOrSpId + (project.getPloidyLevel() == 0 ? ". No ploidy determined at this stage, unable to expand homozygous genotype" : ""));
                continue;    // we don't add invalid genotypes
            }

            try {
	            SampleGenotype aGT = new SampleGenotype(alleles.stream().map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/")));
				GenotypingSample sample = m_providedIdToSampleMap.get(sIndOrSpId);
                Callset callset = m_providedIdToCallsetMap.get(sIndOrSpId);
	        	if (sample == null)
	        		throw new Exception("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
				vrd.getSampleGenotypes().put(callset.getId(), aGT);
            }
            catch (NullPointerException npe) {
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
		return vrd;
    }
}
