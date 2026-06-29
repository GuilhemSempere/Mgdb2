/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2026, <CIRAD> <IRD>
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import fr.cirad.mgdb.importing.base.RefactoredImport;
import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.tools.ProgressIndicator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * Import for AgriPlex genotyping export files (.xlsx).
 *
 * <p>AgriPlex exports are sample-oriented spreadsheets containing (among
 * possibly others) a sheet called "GENOTYPES" with the following layout:</p>
 *
 * <pre>
 *                              [Customer Marker ID]   chr01_194844   chr01_375814  ...   &lt;- optional, row y-3
 *                              AgriPlex ID / SNP_ID   snpOS00039     IRRI_SNP0002  ...   &lt;- marker IDs, row y-2
 *                              Allele_1                A              G            ...   &lt;- row y-1
 * Plate name   Well   Sample_ID  Allele_2              G              T            ...   &lt;- header row y
 * 24_GSL_087_P01  A01   260019                          G              G           ...   &lt;- data rows, y+1..
 * </pre>
 *
 * <p>Since the label used for marker IDs is not consistent across files
 * ("SNP_ID" vs "AgriPlex ID"), we locate the "Plate name" cell at (x, y) and
 * derive every other position relative to it:</p>
 * <ul>
 *   <li>Marker IDs: row y-2, starting at column x+4</li>
 *   <li>Customer Marker IDs (optional): row y-3, starting at column x+4 - if
 *       the cell at (x+3, y-3) does not contain "Customer Marker ID" the row
 *       is considered absent and markers are imported as unplaced</li>
 *   <li>Sample IDs: column x+2 (used as both sample name and individual name)</li>
 *   <li>Genotype data: rows y+1.., columns x+4..</li>
 * </ul>
 *
 * <p>When present, a Customer Marker ID such as "chr01_194844" is converted
 * to a genomic position if (and only if) it strictly matches
 * {@code <STRING>_<NUMBER>} (sequence name "chr01", position 194844);
 * otherwise the corresponding marker is imported as unplaced.</p>
 *
 * <p>Genotype cells are expected to contain either a single nucleotide
 * (collapsed homozygote, e.g. "A"), or two nucleotides separated by a slash
 * (heterozygote, e.g. "A / G"). Any other content (e.g. "-", "FAIL",
 * "MISSING", or any other unrecognized string) is treated as missing data.</p>
 *
 * <p>Like {@link fr.cirad.mgdb.importing.FlapjackImport} and
 * {@link fr.cirad.mgdb.importing.PlinkImport}, the (sample-oriented) genotype
 * matrix is first rotated into a marker-oriented temporary TSV file, which is
 * then fed to {@link RefactoredImport#importTempFileContents}. The xlsx file
 * is read in a streaming fashion using Apache POI's SAX-based XSSFReader, so
 * that its content is never fully loaded into memory. Since both the number
 * of markers (columns) and the number of samples (rows) are unbounded, the
 * transposition is performed by re-reading the sheet once per marker chunk -
 * exactly as {@link fr.cirad.mgdb.importing.FlapjackImport} and
 * {@link fr.cirad.mgdb.importing.PlinkImport} re-read their genotype matrices
 * once per block of markers.</p>
 */
public class AgriplexImport extends RefactoredImport<FileImportParameters> {

    private static final Logger LOG = Logger.getLogger(AgriplexImport.class);

    public static final String DEFAULT_SHEET_NAME = "GENOTYPES";

    /** Number of markers (columns) processed per re-read of the genotype matrix. */
    private static final int MARKER_CHUNK_SIZE = 500;

    /** Matches a sequence of nucleotides (collapsed homozygote). or "-" */
    private static final Pattern HOMOZYGOTE_PATTERN = Pattern.compile("^[ACGTNacgtn]+|-$");

    /** Matches 2 sequences of nucleotides or "-" separated by a slash, with optional surrounding whitespace. */
    private static final Pattern HETEROZYGOTE_PATTERN = Pattern.compile("^\\s*([ACGTNacgtn]+|-)\\s*/\\s*([ACGTNacgtn]+|-)\\s*$");

//    /** Matches a Customer Marker ID of the form &lt;STRING&gt;_&lt;NUMBER&gt;, e.g. "chr01_194844". */
//    private static final Pattern CUSTOMER_MARKER_ID_POSITION_PATTERN = Pattern.compile("^(.+)_([0-9]+)$");

    private File rotatedFile;

    public AgriplexImport() {
    }

    public AgriplexImport(String processID) {
        m_processID = processID;
    }

    public AgriplexImport(boolean fCloseContextAfterImport) {
        this();
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    public AgriplexImport(String processID, boolean fCloseContextAfterImport) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, GENOTYPE xlsx file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

        File genotypeFile = new File(args[4]);
        if (!genotypeFile.exists() || genotypeFile.length() == 0)
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
                false, //fSkipMonomorphic
                mode, //importMode
                (new File(args[4]).toURI().toURL())
        );
        new AgriplexImport().importToMongo(params);
    }

    @Override
    protected long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sModule = params.getModule();
        String sProject = params.getProject();
        String sRun = params.getRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();
        URL genotypeFileURL = params.getMainFileUrl();
        Integer nPloidy = params.getPloidy();
        String sheetName = DEFAULT_SHEET_NAME;

        m_fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

        // First pass: read the header block to determine marker IDs and (optional) positions
        String info = "Reading marker list and positions";
        LOG.info(info);
        progress.addStep(info);
        progress.moveToNextStep();
        LinkedHashMap<String, String> variantsAndPositions = new LinkedHashMap<>();
        Set<String> indelVariants = new HashSet<>();
        AgriplexSheetLayout layout = readMarkersAndPositions(genotypeFileURL, sheetName, variantsAndPositions, indelVariants);
        if (variantsAndPositions.isEmpty()) {
            progress.setError("No marker found in sheet '" + sheetName + "'");
            return 0;
        }


        // Rotate the sample-oriented matrix into a marker-oriented temporary file
        progress.setPercentageEnabled(true);
        info = "Reading and reorganizing genotypes";
        LOG.info(info);
        progress.addStep(info);
        progress.moveToNextStep();
        Map<String, Type> nonSnpVariantTypeMap = new HashMap<>();
        ArrayList<String> individualNames = new ArrayList<>();
        nPloidy = transposeGenotypeFile(genotypeFileURL, sheetName, layout, variantsAndPositions, rotatedFile, nPloidy, nonSnpVariantTypeMap, individualNames, fSkipMonomorphic, progress, indelVariants);
        if (params.getImportMode() == 0 && createdProject == null && project.getPloidyLevel() != nPloidy)
            throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nPloidy + ") data!");
        project.setPloidyLevel(nPloidy);

        if (progress.getError() != null && !progress.isAborted())
            return 0;


        // Create the necessary samples / individuals (Sample_ID is used for both)
        LinkedHashMap<String, String> orderedIndOrSpToPopulationMap = new LinkedHashMap<>();
        for (String sInd : individualNames)
            orderedIndOrSpToPopulationMap.put(sInd, null);    // we have no population info
        progress.setPercentageEnabled(false);
        createCallSetsSamplesIndividuals(new ArrayList<>(orderedIndOrSpToPopulationMap.keySet()), mongoTemplate, project.getId(), sRun, sampleToIndividualMap, progress);
        setSamplesPersisted(true);
        if (progress.getError() != null || progress.isAborted())
            return 0;

        // Rotated file import
        int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nConcurrentThreads + " threads");
        long count = importTempFileContents(progress, nConcurrentThreads, mongoTemplate, assembly == null ? null : assembly.getId(), rotatedFile, variantsAndPositions, existingVariantIDs, project, sRun, null, orderedIndOrSpToPopulationMap, nonSnpVariantTypeMap, null, fSkipMonomorphic);
        if (progress.getError() != null)
            throw new Exception(progress.getError());

        if (progress.isAborted())
            return 0;

        return count;
    }

    @Override
    protected void initReader(FileImportParameters params) throws Exception {
        String path = params.getMainFileUrl().getPath();
        rotatedFile = File.createTempFile("agriplexImport-" + path.substring(path.lastIndexOf('/') + 1) + "-", ".tsv");
    }

    @Override
    protected void closeResource() throws IOException {
        if (rotatedFile != null)
            rotatedFile.delete();
    }


    /**
     * Holds the positions (0-based row/column indices) of the various pieces
     * of information found in an AgriPlex GENOTYPES sheet, all derived from
     * the location of the "Plate name" cell.
     */
    private static class AgriplexSheetLayout {
        /** 0-based column index of the "Plate name" cell. */
        int plateNameCol;
        /** 0-based row index of the "Plate name" cell (= header row, last row of the header block). */
        int headerRow;
        /** 0-based row index containing marker IDs. */
        int markerIdRow;
        /** 0-based row index containing (optional) Customer Marker IDs, or -1 if absent. */
        int sampleIdCol;
        /** 0-based column index of the first marker column. */
        int firstMarkerCol;
        int allele1Row;
        /** 0-based row index containing allele_1. */
        int allele2Row;
        /** 0-based row index containing allele_2. */
    }

    /**
     * First pass over the file: locates the header block, builds the ordered
     * marker list and (when possible) their genomic positions.
     */
    private AgriplexSheetLayout readMarkersAndPositions(URL fileURL, String sheetName, LinkedHashMap<String, String> variantsAndPositionsToFill, Set<String> indelVariants) throws Exception {
        AgriplexSheetLayout layout = new AgriplexSheetLayout();
        layout.plateNameCol = -1;
        layout.headerRow = -1;

        Map<Integer, String> markerIdsByCol = new HashMap<>();
        Map<Integer, String> customerMarkerIdsByCol = new HashMap<>();
        boolean[] customerMarkerIdRowFound = new boolean[]{false};
        Map<Integer, String> markerAllele1ByCol = new HashMap<>();
        Map<Integer, String> markerAllele2ByCol = new HashMap<>();

        // Buffer cells from rows we see before "Plate name" is found,
        // so we can replay them once the layout row indices are known.
        // Key: [col, row], Value: formattedValue
        List<int[]> bufferedCellCoords = new ArrayList<>();
        List<String> bufferedCellValues = new ArrayList<>();

        readSheet(fileURL, sheetName, new AbstractSheetContentsHandler() {

            private void processCell(int col, int row, String formattedValue) {
                if (row > layout.headerRow)
                    return; // past the header block, nothing left to collect

                final int candidateCustomerMarkerIdRow = layout.markerIdRow - 1;

                if (row == layout.markerIdRow && col >= layout.firstMarkerCol && formattedValue != null && !formattedValue.trim().isEmpty()) {
                    markerIdsByCol.put(col, formattedValue.trim());
                } else if (row == candidateCustomerMarkerIdRow) {
                    if (col == layout.plateNameCol + 3 && "Customer Marker ID".equals(formattedValue))
                        customerMarkerIdRowFound[0] = true;
                    else if (col >= layout.firstMarkerCol && formattedValue != null && !formattedValue.trim().isEmpty())
                        customerMarkerIdsByCol.put(col, formattedValue.trim());
                } else if (row == layout.allele1Row && col >= layout.firstMarkerCol && formattedValue != null && !formattedValue.trim().isEmpty()) {
                    markerAllele1ByCol.put(col, formattedValue.trim());
                } else if (row == layout.allele2Row && col >= layout.firstMarkerCol && formattedValue != null && !formattedValue.trim().isEmpty()) {
                    markerAllele2ByCol.put(col, formattedValue.trim());
                }
            }

            @Override
            public void cell(String cellReference, String formattedValue, XSSFComment comment) {
                int[] colRow = colRowFromCellReference(cellReference);
                int col = colRow[0], row = colRow[1];

                if (layout.plateNameCol == -1) {
                    // Layout not yet known — buffer this cell
                    bufferedCellCoords.add(new int[]{col, row});
                    bufferedCellValues.add(formattedValue);

                    if ("Plate name".equals(formattedValue)) {
                        layout.plateNameCol = col;
                        layout.headerRow = row;
                        layout.sampleIdCol = col + 2;
                        layout.markerIdRow = row - 2;
                        layout.firstMarkerCol = col + 4;
                        layout.allele1Row = row - 1;
                        layout.allele2Row = row;

                        // Replay all buffered cells now that row indices are known
                        for (int i = 0; i < bufferedCellCoords.size(); i++)
                            processCell(bufferedCellCoords.get(i)[0], bufferedCellCoords.get(i)[1], bufferedCellValues.get(i));

                        bufferedCellCoords.clear();
                        bufferedCellValues.clear();
                    }
                    return;
                }

                processCell(col, row, formattedValue);
            }
        });

        if (layout.plateNameCol == -1)
            throw new Exception("Could not find a 'Plate name' cell in sheet '" + sheetName + "'");

        if (!customerMarkerIdRowFound[0])
            customerMarkerIdsByCol.clear();

        if (markerIdsByCol.isEmpty())
            throw new Exception("No marker ID found at row " + (layout.markerIdRow + 1) + " of sheet '" + sheetName + "'");

        List<Integer> orderedCols = markerIdsByCol.keySet().stream().sorted().collect(Collectors.toList());
        for (Integer col : orderedCols) {
            String markerId = markerIdsByCol.get(col);
            if (markerAllele1ByCol.get(col).equals("-") || markerAllele2ByCol.get(col).equals("-"))
                indelVariants.add(markerId);
            variantsAndPositionsToFill.put(markerId, null);
        }

        return layout;
    }

    /**
     * Second pass(es) over the file: rotates the sample-oriented genotype
     * matrix into a marker-oriented temporary TSV file (one line per marker,
     * one tab-separated genotype per sample, in the order in which samples
     * were first encountered).
     *
     * <p>Since neither the number of markers nor the number of samples is
     * bounded, the matrix is processed in chunks of
     * {@link #MARKER_CHUNK_SIZE} markers: for each chunk, the sheet is
     * streamed again and only the genotypes belonging to that chunk are kept
     * in memory (one {@link StringBuilder} per marker of the chunk), then
     * immediately appended to the output file. Sample IDs are collected once,
     * during the first chunk.</p>
     *
     * @return the dataset's ploidy (2, since AgriPlex only ever provides
     *         single-nucleotide or two-allele genotypes)
     */
    private int transposeGenotypeFile(URL fileURL, String sheetName, AgriplexSheetLayout layout, LinkedHashMap<String, String> variantsAndPositions, File outputFile, Integer nProvidedPloidy, Map<String, Type> nonSnpVariantTypeMapToFill, ArrayList<String> individualListToFill, boolean fSkipMonomorphic, ProgressIndicator progress, Set<String> indelMarkers) throws Exception {
        long before = System.currentTimeMillis();

        String[] markerIds = variantsAndPositions.keySet().toArray(new String[0]);
        int markerCount = markerIds.length;
        int ploidy = 2;    // AgriPlex genotypes are typically diploid (single nucleotide or X/Y)

        // One StringBuilder per marker, built in a single pass
        final StringBuilder[] transposed = new StringBuilder[markerCount];
        for (int i = 0; i < markerCount; i++)
            transposed[i] = new StringBuilder();

        @SuppressWarnings("unchecked")
        final Set<String>[] distinctAlleles = new LinkedHashSet[markerCount];
        for (int i = 0; i < markerCount; i++)
            distinctAlleles[i] = new LinkedHashSet<>();

        readSheet(fileURL, sheetName, new AbstractSheetContentsHandler() {
            String currentSampleId = null;
            boolean currentRowHasData = false;
            final String[] currentRowGenotypes = new String[markerCount];

            @Override
            public void startRow(int rowNum) {
                if (rowNum > layout.headerRow) {
                    currentSampleId = null;
                    currentRowHasData = false;
                    Arrays.fill(currentRowGenotypes, null);
                }
            }

            @Override
            public void endRow(int rowNum) {
                if (rowNum <= layout.headerRow)
                    return;
                if (currentSampleId == null || currentSampleId.trim().isEmpty()) {
                    if (currentRowHasData)
                        LOG.warn("Skipping row " + (rowNum + 1) + ": no Sample_ID found");
                    return;
                }
                individualListToFill.add(currentSampleId.trim());
                for (int i = 0; i < markerCount; i++) {
                    transposed[i].append("\t");
                    if (currentRowGenotypes[i] != null) {
                        transposed[i].append(currentRowGenotypes[i]);
                        for (String allele : currentRowGenotypes[i].split("/"))
                            distinctAlleles[i].add(allele);
                    }
                }
            }

            @Override
            public void cell(String cellReference, String formattedValue, XSSFComment comment) {
                int[] colRow = colRowFromCellReference(cellReference);
                int col = colRow[0], row = colRow[1];
                if (row <= layout.headerRow)
                    return;
                if (col == layout.sampleIdCol) {
                    currentSampleId = formattedValue;
                    currentRowHasData = true;
                    return;
                }
                if (col < layout.firstMarkerCol || col >= layout.firstMarkerCol + markerCount)
                    return;
                currentRowHasData = true;
                int idx = col - layout.firstMarkerCol;
                currentRowGenotypes[idx] = normalizeGenotype(formattedValue, indelMarkers.contains(markerIds[idx]));
            }
        });

        if (individualListToFill.isEmpty())
            throw new Exception("No sample found in sheet '" + sheetName + "'");

        // Write TSV in one go
        try (BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFile))) {
            for (int i = 0; i < markerCount; i++) {
                String variantName = markerIds[i];
                if (!distinctAlleles[i].isEmpty()) {
                    List<Allele> alleleList = distinctAlleles[i].stream()
                            .map(a -> Allele.create(a))
                            .collect(Collectors.toList());
                    Type variantType = determineType(alleleList);
                    if (variantType != Type.SNP)
                        nonSnpVariantTypeMapToFill.put(variantName, variantType);
                }
                outputWriter.write(variantName);
                outputWriter.write("\t");
                outputWriter.write(transposed[i].length() > 0 ? transposed[i].substring(1) : "");
                outputWriter.newLine();
            }
        }

        LOG.info("Genotype matrix transposition took " + (System.currentTimeMillis() - before) + "ms for "
                + markerCount + " markers and " + individualListToFill.size() + " samples");

        Runtime.getRuntime().gc();
        return nProvidedPloidy != null ? nProvidedPloidy : ploidy;
    }


    /**
     * Converts a raw genotype cell value into the tab-file representation
     * expected by {@link RefactoredImport#importTempFileContents}: a single
     * allele for collapsed homozygotes, "allele1/allele2" for heterozygotes,
     * or {@code null} for anything else (missing data: "-", "FAIL",
     * "MISSING", empty cell, or any other unrecognized content).
     */
    static String normalizeGenotype(String rawValue, boolean isIndel) {
        if (rawValue == null)
            return null;

        String value = rawValue.trim();
        if (value.isEmpty())
            return null;

        if (HOMOZYGOTE_PATTERN.matcher(value).matches()) {
            if (value.equals("-")) {
                if (isIndel)
                    return "N";
                else
                    return null; //missing data
            } else if (isIndel) {
                return "NN";
            } else {
                return value.toUpperCase();
            }
        }

        Matcher hetMatcher = HETEROZYGOTE_PATTERN.matcher(value);
        if (hetMatcher.matches()) {
            String gt;
            if (hetMatcher.group(1).equals("-")) {
                if (isIndel)
                    gt = "N"; //e.g. -/A
                else
                    return null; // - means missing here
            } else if (isIndel) {
                gt = "NN"; //e.g. AT/-
            } else {
                gt = hetMatcher.group(1).toUpperCase(); //e.g. TT/AA
            }
            gt = gt + "/";
            if (hetMatcher.group(2).equals("-")) {
                if (isIndel)
                    gt = gt + "N"; //e.g. A/-
                else
                    return null;
            } else if (isIndel) {
                gt = gt + "NN"; //e.g. -/AT
            } else {
                gt = gt + hetMatcher.group(2).toUpperCase(); //e.g. TT/AA
            }
            return gt;
        }

        // Anything else ("-", "FAIL", "MISSING", multi-character strings, etc.) is missing data
        return null;
    }


    /**
     * Streams the given sheet of an xlsx file using Apache POI's SAX-based
     * {@link XSSFReader}, without ever loading the whole workbook into memory.
     *
     * Opening OPCPackage from a File (rather than an InputStream) lets POI use
     * ZipFile-based random access, avoiding the full in-memory buffer that
     * ZipInputStreamZipEntrySource requires and that trips the 100 MB limit.
     */
    private static void readSheet(URL fileURL, String sheetName, SheetContentsHandler handler) throws Exception {
        File localFile;
        Path tempFile = null;
        if ("file".equalsIgnoreCase(fileURL.getProtocol())) {
            localFile = new File(fileURL.toURI());
        } else {
            tempFile = Files.createTempFile("agriplexImport_", ".xlsx");
            try (InputStream is = fileURL.openStream()) {
                Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }
            localFile = tempFile.toFile();
        }

        try (OPCPackage pkg = OPCPackage.open(localFile)) {
            XSSFReader reader = new XSSFReader(pkg);
            StylesTable styles = reader.getStylesTable();
            ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
            DataFormatter formatter = new DataFormatter();

            XSSFReader.SheetIterator sheetIterator = (XSSFReader.SheetIterator) reader.getSheetsData();
            InputStream sheetStream = null;
            try {
                while (sheetIterator.hasNext()) {
                    InputStream candidate = sheetIterator.next();
                    if (sheetName.equals(sheetIterator.getSheetName())) {
                        sheetStream = candidate;
                        break;
                    } else
                        candidate.close();
                }

                if (sheetStream == null)
                    throw new Exception("Sheet '" + sheetName + "' not found");

                SAXParserFactory saxFactory = SAXParserFactory.newInstance();
                saxFactory.setNamespaceAware(true);
                XMLReader sheetParser = saxFactory.newSAXParser().getXMLReader();
                ContentHandler contentHandler = new XSSFSheetXMLHandler(styles, null, strings, handler, formatter, false);
                sheetParser.setContentHandler(contentHandler);
                sheetParser.parse(new InputSource(sheetStream));
            } finally {
                if (sheetStream != null)
                    sheetStream.close();
            }
        } finally {
            if (tempFile != null)
                Files.deleteIfExists(tempFile);
        }
    }

    /**
     * Converts an A1-style cell reference (e.g. "AB12") into 0-based
     * [column, row] indices.
     */
    private static int[] colRowFromCellReference(String cellReference) {
        int i = 0;
        while (i < cellReference.length() && Character.isLetter(cellReference.charAt(i)))
            i++;

        String colPart = cellReference.substring(0, i);
        String rowPart = cellReference.substring(i);

        int col = 0;
        for (int j = 0; j < colPart.length(); j++)
            col = col * 26 + (colPart.charAt(j) - 'A' + 1);
        col -= 1;    // 0-based

        int row = Integer.parseInt(rowPart) - 1;    // 0-based

        return new int[] { col, row };
    }


    /**
     * Convenience base class providing no-op implementations of the
     * {@link SheetContentsHandler} methods that are not needed by most
     * handlers in this class.
     */
    private static abstract class AbstractSheetContentsHandler implements SheetContentsHandler {
        @Override
        public void startRow(int rowNum) {
        }

        @Override
        public void endRow(int rowNum) {
        }

        @Override
        public void headerFooter(String text, boolean isHeader, String tagName) {
        }
    }
}