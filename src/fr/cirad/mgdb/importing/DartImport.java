package fr.cirad.mgdb.importing;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
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

import fr.cirad.mgdb.model.mongo.maintypes.*;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class DartImport extends AbstractGenotypeImport {

    /** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);

	/** The m_process id. */
	private String m_processID;

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

    public DartIterator getDartInfo(String path) throws Exception {
//        FileReader fileReader = new FileReader(path);

        Scanner scanner = new Scanner(new File(path));
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


    public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, URL mainFileUrl, String assemblyName, HashMap<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode) throws Exception
    {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
        progress.setPercentageEnabled(false);

        Integer createdProject = null;

        //FeatureReader<RawHapMapFeature> reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), new RawHapMapCodec(), false);
        DartIterator dartIterator = getDartInfo(URLDecoder.decode(mainFileUrl.getPath(), "UTF-8"));

//        BufferedReader bufferedReader = new BufferedReader(new FileReader(URLDecoder.decode(mainFileUrl.getPath(), "UTF-8")));
        
        
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
//                    Iterator<DartInfo> it = reader.iterator();
//                    dartFeatures;
                    variantLoop: while (nTestedVariantCount < 1000 && dartIterator.hasNext()) {
                    	List<DartInfo> dartFeatures = dartIterator.next();
                    	for (DartInfo dartFeature : dartFeatures)
	                        if (dartFeature.getAlleles().length > 1) {
	                            project.setPloidyLevel(dartFeature.getAlleles().length);
	                            LOG.info("Guessed ploidy level for dataset to import: " + project.getPloidyLevel());
	                            break variantLoop;
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

//            Iterator<DartInfo> it = reader.iterator();
            DartIterator dataReader = getDartInfo(URLDecoder.decode(mainFileUrl.getPath(), "UTF-8"));
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

                                        HashSet<Individual> indsToAdd = new HashSet<>();
                                        boolean fDbAlreadyContainedIndividuals = finalMongoTemplate.findOne(new Query(), Individual.class) != null;
                                        for (String sIndOrSpId : sampleIds) {
                                            String sIndividual = sampleToIndividualMap == null ? sIndOrSpId : sampleToIndividualMap.get(sIndOrSpId);
                                            if (sIndividual == null) {
                                                progress.setError("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
                                                return;
                                            }

                                            if (!fDbAlreadyContainedIndividuals && finalMongoTemplate.findById(sIndividual, Individual.class) == null)  // we don't have any population data so we don't need to update the Individual if it already exists
                                                indsToAdd.add(new Individual(sIndividual));

                                            int sampleId = AutoIncrementCounter.getNextSequence(finalMongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                                            m_providedIdToSampleMap.put(sIndOrSpId, new GenotypingSample(sampleId, finalProject.getId(), sRun, sIndividual, sampleToIndividualMap == null ? null : sIndOrSpId));   // add a sample for this individual to the project
                                        }
                                        
                                        // make sure provided sample names do not conflict with existing ones
                                        if (finalMongoTemplate.findOne(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).in(m_providedIdToSampleMap.values().stream().map(sp -> sp.getSampleName()).toList())), GenotypingSample.class) != null) {
                                            progress.setError("Some of the sample IDs provided in the mapping file already exist in this database!");
                                            return;
                                        }

                                        final int importChunkSize = 1000;
                                        Thread sampleImportThread = new Thread() {
                                            public void run() {                 
                                                List<GenotypingSample> samplesToImport = new ArrayList<>(m_providedIdToSampleMap.values());
                                                for (int j=0; j<Math.ceil((float) m_providedIdToSampleMap.size() / importChunkSize); j++)
                                                    finalMongoTemplate.insert(samplesToImport.subList(j * importChunkSize, Math.min(samplesToImport.size(), (j + 1) * importChunkSize)), GenotypingSample.class);
                                                samplesToImport = null;
                                            }
                                        };
                                        sampleImportThread.start();

                                        List<Individual> individualsToImport = new ArrayList<>(indsToAdd);
                                        for (int j=0; j<Math.ceil((float) individualsToImport.size() / importChunkSize); j++)
                                        	finalMongoTemplate.insert(individualsToImport.subList(j * importChunkSize, Math.min(individualsToImport.size(), (j + 1) * importChunkSize)), Individual.class);
                                        individualsToImport = null;

                                        sampleImportThread.join();
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

                                    VariantRunData runToSave = addDartSeqDataToVariant(finalMongoTemplate, variant, finalAssembly == null ? null : finalAssembly.getId(), variantType, alleleIndexMap, dartFeature, finalProject, sRun, sampleIds);

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
                return createdProject;

            // save project data
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);

            LOG.info("DartSeqImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + totalProcessedVariantCount.get() + " records");

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

            dartIterator.close();

            MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
            if (progress.getError() == null && !progress.isAborted()) {
                progress.addStep("Preparing database for searches");
                progress.moveToNextStep();
                MgdbDao.prepareDatabaseForSearches(sModule);
            }
        }
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
     * @return the variant run data
     * @throws Exception the exception
     */
    private VariantRunData addDartSeqDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId, Type variantType, Map<String, Integer> alleleIndexMap, DartInfo dartFeature, GenotypingProject project, String runName, List<String>individuals) throws Exception {
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
	        	if (sample == null)
	        		throw new Exception("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
				vrd.getSampleGenotypes().put(sample.getId(), aGT);
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
        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setPositions(variantToFeed.getPositions());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
		return vrd;
    }
}
