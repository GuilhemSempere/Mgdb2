package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.importing.parameters.DartImportParameters;
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
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

public class DartImport extends AbstractGenotypeImport<DartImportParameters> {

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
                String alleleID1 = line.split(",")[0].split("[^0-9]")[0], snpPos1 = line.split(",")[fieldPositions.get("SnpPosition")];
                String line1 = line;
                line = scanner.nextLine();
                String alleleID2 = line.split(",")[0].split("[^0-9]")[0], snpPos2 = line.split(",")[fieldPositions.get("SnpPosition")];
                twoRow = alleleID1.equals(alleleID2) && snpPos1.equals(snpPos2);    // a clone may carry several SNPs, each on its own line
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
     * Streams the read counts of a DArTseq .dartcounts file, sorted by CloneID,
     * alongside genotype lines sorted the same way (see {@link SortedDartseqPair}),
     * so that only the counts lines of the current clone are ever held in memory.
     *
     * <p>The counts file has one line per allele cluster: a sample column holds the
     * reads supporting that line's allele, and columns suffixed with .N are technical
     * replicates, pooled into their sample unless the genotype file lists them as samples.</p>
     */
    static final class DartCountsReader implements AutoCloseable {
        private static final Pattern SNP_IN_ALLELE_ID = Pattern.compile("(\\d+):([ACGT])>([ACGT])");

        final int cloneIdCol;
        private final int alleleIdCol, snpCol, alleleSeqCol;
        private final Map<String, int[]> countColumnsBySample = new LinkedHashMap<>();
        private SortedDartseqPair.LineIterator lines;
        private String currentClone;
        private final List<String[]> currentCloneLines = new ArrayList<>();

        DartCountsReader(String headerLine, Collection<String> genotypedSamples) throws Exception {
            List<String> headers = Arrays.stream(headerLine.split(",", -1)).map(String::trim).collect(Collectors.toList());
            cloneIdCol = headers.indexOf("CloneID");
            alleleIdCol = headers.indexOf("AlleleID");
            snpCol = headers.indexOf("SNP");
            alleleSeqCol = headers.indexOf("AlleleSequence");
            int rdepthCol = headers.indexOf("rdepth");    // last metadata column, sample columns follow
            if (cloneIdCol == -1 || alleleIdCol == -1 || snpCol == -1 || rdepthCol == -1)
                throw new Exception("Read-counts file must contain CloneID, AlleleID, SNP and rdepth columns");

            Map<String, List<Integer>> columnsBySample = new LinkedHashMap<>();
            for (int i = rdepthCol + 1; i < headers.size(); i++) {
                String column = headers.get(i), sample = genotypedSamples.contains(column) ? column : column.replaceFirst("\\.\\d+$", "");
                if (genotypedSamples.contains(sample))
                    columnsBySample.computeIfAbsent(sample, k -> new ArrayList<>()).add(i);
                else if (!column.isEmpty())
                    LOG.warn("Read-counts column " + column + " matches no genotyped sample, ignoring it");
            }
            for (Map.Entry<String, List<Integer>> entry : columnsBySample.entrySet())
                countColumnsBySample.put(entry.getKey(), entry.getValue().stream().mapToInt(Integer::intValue).toArray());
        }

        void setLines(SortedDartseqPair.LineIterator lines) {
            this.lines = lines;
        }

        /**
         * @return sample -> [reads supporting the line's REF allele, reads supporting its ALT allele], or null if the marker has no read counts.
         * Must be called with genotype lines in CloneID order.
         */
        Map<String, int[]> countsFor(DartInfo dartFeature) throws IOException {
            String cloneId = dartFeature.getAlleleID().trim().split("\\|")[0];
            if (!cloneId.equals(currentClone)) {
                if (currentClone != null && cloneId.compareTo(currentClone) < 0)
                    throw new IOException("Genotype lines are not sorted by CloneID (" + cloneId + " after " + currentClone + ")");
                currentClone = cloneId;
                currentCloneLines.clear();
                while (lines.hasNext()) {
                    int cmp = SortedDartseqPair.extractField(lines.peek(), cloneIdCol).compareTo(cloneId);
                    if (cmp > 0)
                        break;
                    String line = lines.next();
                    if (cmp == 0)
                        currentCloneLines.add(line.split(",", -1));
                }
            }

            char ref = dartFeature.getAlleles()[0].charAt(0), alt = dartFeature.getAlleles()[1].charAt(0);
            Map<String, int[]> readsBySample = null;
            for (String[] fields : currentCloneLines) {
                int alleleIndex = supportedAllele(fields, dartFeature.getSnpPos(), ref, alt);
                if (alleleIndex == -1)
                    continue;
                for (Map.Entry<String, int[]> entry : countColumnsBySample.entrySet()) {
                    int reads = 0;
                    for (int col : entry.getValue())
                        reads += parseCount(field(fields, col));
                    if (reads > 0) {
                        if (readsBySample == null)
                            readsBySample = new HashMap<>();
                        readsBySample.computeIfAbsent(entry.getKey(), k -> new int[2])[alleleIndex] += reads;
                    }
                }
            }
            return readsBySample;
        }

        /** @return 0 if a counts line supports the REF allele of the given SNP, 1 if it supports its ALT allele, -1 if it describes another SNP */
        private int supportedAllele(String[] fields, int snpPos, char ref, char alt) {
            Matcher m = SNP_IN_ALLELE_ID.matcher(field(fields, alleleIdCol));
            if (!m.find() || Integer.parseInt(m.group(1)) != snpPos || m.group(2).charAt(0) != ref || m.group(3).charAt(0) != alt)
                return -1;

            // The allele this cluster carries: its sequence at SnpPosition (0-based within the tag)...
            String alleleSequence = field(fields, alleleSeqCol);
            if (snpPos < alleleSequence.length()) {
                char base = Character.toUpperCase(alleleSequence.charAt(snpPos));
                if (base == ref)
                    return 0;
                if (base == alt)
                    return 1;
            }
            // ... or, failing that, DArT's convention: the REF cluster line leaves the SNP column empty
            return field(fields, snpCol).isEmpty() ? 0 : 1;
        }

        private static String field(String[] fields, int col) {
            return col >= 0 && col < fields.length ? fields[col].trim() : "";
        }

        private static int parseCount(String value) {
            try {
                return value.isEmpty() ? 0 : Math.max(0, (int) Math.round(Double.parseDouble(value)));
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        @Override
        public void close() throws IOException {
            if (lines != null)
                lines.close();
        }
    }

    /** Returns the first line of a DArT file that is not a "*," banner line, i.e. its header. */
    private static String readHeaderLine(File file) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String line = br.readLine();
            while (line != null && line.startsWith("*,"))
                line = br.readLine();
            if (line == null)
                throw new IOException("File is empty: " + file.getName());
            return line;
        }
    }

    /** Returns a local file for the given URL, downloading it to a temporary file (added to tempFiles) if it is remote. */
    private static File toLocalFile(URL url, List<File> tempFiles) throws Exception {
        if ("file".equals(url.getProtocol()))
            return new File(url.toURI());
        File localCopy = File.createTempFile("dartImport-", ".csv");
        tempFiles.add(localCopy);
        try (InputStream is = url.openStream()) {
            Files.copy(is, localCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return localCopy;
    }

    /**
     * Task class for dispatching variant processing
     */
    private static class VariantTask {
        public static final VariantTask POISON_PILL = new VariantTask(null, null, null);
        
        final DartInfo dartFeature;
        final String variantId;
        final Map<String, int[]> readCounts;   // sample -> [REF reads, ALT reads], null if no counts
        
        VariantTask(DartInfo dartFeature, String variantId, Map<String, int[]> readCounts) {
            this.dartFeature = dartFeature;
            this.variantId = variantId;
            this.readCounts = readCounts;
        }
    }

    @Override
    protected long doImport(DartImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
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
        AtomicInteger totalWrittenVariantCount = new AtomicInteger(0);
        final ArrayList<String> sampleIds = new ArrayList<>();

        // With a read-counts file, sort both files by CloneID on disk so that each marker's counts can be joined to it while streaming
        SortedDartseqPair sortedFiles = null;
        DartCountsReader countsReader = null;
        if (params.getCountsFileUrl() != null) {
            progress.addStep("Sorting genotype and read-count files by marker");
            progress.moveToNextStep();
            List<File> downloadedFiles = new ArrayList<>();
            try {
                File genotypeFile = toLocalFile(params.getMainFileUrl(), downloadedFiles), countsFile = toLocalFile(params.getCountsFileUrl(), downloadedFiles);
                String genotypeHeader = readHeaderLine(genotypeFile), countsHeader = readHeaderLine(countsFile);
                List<String> genotypeColumns = Arrays.asList(genotypeHeader.split(","));
                if (!genotypeColumns.contains("CloneID") || !genotypeColumns.contains("RepAvg"))
                    throw new Exception("Genotype file must contain CloneID and RepAvg columns to be imported with read counts");
                countsReader = new DartCountsReader(countsHeader, genotypeColumns.subList(genotypeColumns.indexOf("RepAvg") + 1, genotypeColumns.size()));
                sortedFiles = SortedDartseqPair.sort(genotypeFile, genotypeColumns.indexOf("CloneID"), countsFile, countsReader.cloneIdCol, progress);
                countsReader.setLines(sortedFiles.counts());
            } finally {
                for (File f : downloadedFiles)
                    f.delete();
            }
        }

        progress.addStep("Processing variant lines");
        progress.moveToNextStep();

        int nNConcurrentThreads = Math.max(1, nNumProc);
        int nImportThreads = Math.max(1, (nNConcurrentThreads - 1) / 2);
        LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nImportThreads + " threads");

        DartIterator dataReader = getDartInfo(sortedFiles == null ? params.getMainFileUrl() : sortedFiles.getSortedGenotypeFile().toURI().toURL());
        
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

        // Setup VCF header
        VCFInfoHeaderLine headerLineGT = new VCFInfoHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
        VCFInfoHeaderLine headerLineAS = new VCFInfoHeaderLine("AS", 2, VCFHeaderLineType.String, "AlleleSequence");
        VCFInfoHeaderLine headerLineSP = new VCFInfoHeaderLine("SP", 3, VCFHeaderLineType.Integer, "SnpPosition");
        VCFInfoHeaderLine headerLineCR = new VCFInfoHeaderLine("CR", 4, VCFHeaderLineType.Float, "CallRate");
        VCFInfoHeaderLine headerLineFHR = new VCFInfoHeaderLine("FHR", 5, VCFHeaderLineType.Float, "FreqHomRef");
        VCFInfoHeaderLine headerLineFHS = new VCFInfoHeaderLine("FHS", 6, VCFHeaderLineType.Float, "FreqHomSnp");
        VCFInfoHeaderLine headerLineFH = new VCFInfoHeaderLine("FH", 7, VCFHeaderLineType.Float, "FreqHet");

        HashSet<VCFHeaderLine> headerLines = new HashSet<>(Arrays.asList(headerLineGT, headerLineAS, headerLineSP, headerLineCR, headerLineFHR, headerLineFHS, headerLineFH));
        if (countsReader != null) {
            headerLines.add(new VCFFormatHeaderLine(VariantData.GT_FIELD_AD, VCFHeaderLineCount.R, VCFHeaderLineType.Integer, "Allelic depths for the ref and alt alleles in the order listed"));
            headerLines.add(new VCFFormatHeaderLine(VariantData.GT_FIELD_DP, 1, VCFHeaderLineType.Integer, "Read depth"));
        }
        VCFHeader header = new VCFHeader(headerLines);
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
                            totalWrittenVariantCount,
                            existingVariantIDs,
                            fSkipMonomorphic,
                            sampleIds,
                            sampleToIndividualMap,
                            sharedVariantCache
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
                                variantId = generateFallbackVariantId();
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
                        VariantTask task = new VariantTask(dartFeature, variantId, countsReader == null ? null : countsReader.countsFor(dartFeature));
                        workerQueues[workerIndex].put(task);
                        
                    } catch (Exception e) {
                        LOG.error("Error processing variant: " + e.getMessage(), e);
                        progress.setError("Error processing variant: " + e.getMessage());
                        break;  // the error stops the dispatch loop, then workers are released and temp files cleaned up below
                    }
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

        dataReader.close();
        if (countsReader != null)
            countsReader.close();
        if (sortedFiles != null)
            sortedFiles.close();

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
            AtomicInteger totalWrittenVariantCount,
            HashMap<String, String> existingVariantIDs,
            boolean fSkipMonomorphic,
            ArrayList<String> sampleIds,
            Map<String, String> sampleToIndividualMap,
            ConcurrentHashMap<String, VariantData> sharedVariantCache) throws Exception {
        
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
                existingVariantIDs,
                task.readCounts
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
            
            workerProcessed++;            
            if (workerProcessed % chunkSize == 0 && !unsavedVariants.isEmpty()) {
                persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, 
                    unsavedVariants, unsavedRuns);
                progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size()));
                
                unsavedVariants = new HashSet<>();
                unsavedRuns = new HashSet<>();
            }
        }
        
        // Save remaining
        if (!unsavedVariants.isEmpty()) {
            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, 
                unsavedVariants, unsavedRuns);
            progress.setCurrentStepProgress(totalWrittenVariantCount.addAndGet(unsavedVariants.size()));
        }
    }

    @Override
    protected void initReader(DartImportParameters params) throws Exception {
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
            HashMap<String, String> existingVariantIDs,
            Map<String, int[]> readCounts) throws Exception {
        
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
                int[] sampleReads = readCounts == null ? null : readCounts.get(sIndOrSpId);
                if (sampleReads != null) {
                    int[] ad = new int[variantToFeed.getKnownAlleles().size()];
                    ad[alleleIndexMap.get(dartFeature.getAlleles()[0])] = sampleReads[0];
                    ad[alleleIndexMap.get(dartFeature.getAlleles()[1])] = sampleReads[1];
                    aGT.getAdditionalInfo().put(VariantData.GT_FIELD_AD, Helper.arrayToCsv(",", ad));
                    aGT.getAdditionalInfo().put(VariantData.GT_FIELD_DP, sampleReads[0] + sampleReads[1]);
                }
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