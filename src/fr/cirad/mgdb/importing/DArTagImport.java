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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.importing.base.RefactoredImport;
import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.tools.ProgressIndicator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * Import for DArTag genotyping export files (.csv).
 *
 * <p>DArTag exports are sample-oriented CSV files: each data row is a sample
 * and each marker occupies a column. The file structure is:</p>
 * <ul>
 *   <li>Row 1 (header): {@code PLATE_ID, WELL, SUBJECT_ID, <markerID>, <markerID>, ...}</li>
 *   <li>Rows 2..n (data): one row per sample, genotypes colon-separated (e.g. {@code "A:G"})</li>
 * </ul>
 *
 * <p>The {@code SUBJECT_ID} column value is used as both sample name and
 * individual name. No genomic position information is present in this format;
 * all markers are imported as unplaced.</p>
 *
 * <p>Genotype format: two alleles separated by a colon, e.g. {@code "A:G"} for
 * a heterozygote or {@code "A:A"} for a homozygote. Empty cells or {@code "-"}
 * are treated as missing data.</p>
 *
 * <p>Because the file is sample-oriented, the genotype matrix is transposed
 * into a marker-oriented temporary TSV file (one line per marker, tab-separated
 * genotypes in sample order), which is then fed to
 * {@link RefactoredImport#importTempFileContents}. To avoid loading the entire
 * matrix into memory, the file is re-read once per chunk of
 * {@link #MARKER_CHUNK_SIZE} columns, exactly as
 * {@link AgriplexImport} does for its xlsx input.</p>
 */
public class DArTagImport extends RefactoredImport<FileImportParameters> {

    private static final Logger LOG = Logger.getLogger(DArTagImport.class);

    /** Column index (0-based) of the SUBJECT_ID field in every row. */
    private static final int SUBJECT_ID_COL = 2;

    /** Column index (0-based) of the first marker in the header row. */
    private static final int FIRST_MARKER_COL = 3;

    /** Number of markers (columns) transposed per re-read of the file. */
    private static final int MARKER_CHUNK_SIZE = 500;

    private File rotatedFile;

    public DArTagImport() {
    }

    public DArTagImport(String processID) {
        m_processID = processID;
    }

    public DArTagImport(boolean fCloseContextAfterImport) {
        this();
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    public DArTagImport(boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this();
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    public DArTagImport(String processID, boolean fCloseContextAfterImport) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
    }

    public DArTagImport(String processID, boolean fCloseContextAfterImport, boolean fAllowNewAssembly) {
        this(processID);
        m_fCloseContextAfterImport = fCloseContextAfterImport;
        m_fAllowNewAssembly = fAllowNewAssembly;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 6)
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, DArTag CSV file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

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
                null,    //nPloidy
                args[5], //assemblyName
                null,    //sampleToIndividualMap
                false,   //fSkipMonomorphic
                mode,    //importMode
                new File(args[4]).toURI().toURL()
        );
        new DArTagImport().importToMongo(params);
    }

    @Override
    protected void initReader(FileImportParameters params) throws Exception {
        String path = params.getMainFileUrl().getPath();
        rotatedFile = File.createTempFile("dartag-" + path.substring(path.lastIndexOf('/') + 1) + "-", ".tsv");
    }

    @Override
    protected void closeResource() throws IOException {
        if (rotatedFile != null)
            rotatedFile.delete();
    }

    @Override
    protected long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        URL fileURL = params.getMainFileUrl();
        String sRun = params.getRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();

        m_fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(params.getModule());

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());

        // First pass: read the header row to collect marker IDs and validate structure
        String info = "Reading marker list";
        LOG.info(info);
        progress.addStep(info);
        progress.moveToNextStep();

        LinkedHashMap<String, String> variantsAndPositions = new LinkedHashMap<>();
        ArrayList<String> individualNames = new ArrayList<>();

        readMarkerIds(fileURL, variantsAndPositions);
        if (variantsAndPositions.isEmpty()) {
            progress.setError("No marker columns found in DArTag file");
            return 0;
        }

        // Transpose the sample-oriented matrix into a marker-oriented temp file
        progress.setPercentageEnabled(true);
        info = "Reading and reorganizing genotypes";
        LOG.info(info);
        progress.addStep(info);
        progress.moveToNextStep();

        Map<String, Type> nonSnpVariantTypeMap = new HashMap<>();
        transposeGenotypeFile(fileURL, variantsAndPositions, rotatedFile, nonSnpVariantTypeMap, individualNames, progress);

        if (individualNames.isEmpty()) {
            progress.setError("No sample rows found in DArTag file");
            return 0;
        }

        // DArTag genotypes are always two explicit alleles: ploidy is 2
        int nPloidy = params.getPloidy() != null ? params.getPloidy() : 2;
        if (params.getImportMode() == 0 && createdProject == null && project.getPloidyLevel() != nPloidy)
            throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nPloidy + ") data!");
        project.setPloidyLevel(nPloidy);
        m_ploidy = nPloidy;

        if (progress.getError() != null || progress.isAborted())
            return 0;

        // Create samples — SUBJECT_ID is used as both sample name and individual name
        LinkedHashMap<String, String> orderedIndOrSpToPopulationMap = new LinkedHashMap<>();
        for (String sInd : individualNames)
            orderedIndOrSpToPopulationMap.put(sInd, null);
        progress.setPercentageEnabled(false);
        createCallSetsSamplesIndividuals(new ArrayList<>(orderedIndOrSpToPopulationMap.keySet()), mongoTemplate, project.getId(), sRun, sampleToIndividualMap, progress);
        setSamplesPersisted(true);
        if (progress.getError() != null || progress.isAborted())
            return 0;

        int nConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        LOG.debug("Importing project '" + params.getProject() + "' into " + params.getModule() + " using " + nConcurrentThreads + " threads");

        long count = importTempFileContents(progress, nConcurrentThreads, mongoTemplate, assembly == null ? null : assembly.getId(), rotatedFile, variantsAndPositions, existingVariantIDs, project, sRun, null, orderedIndOrSpToPopulationMap, nonSnpVariantTypeMap, null, fSkipMonomorphic);
        if (progress.getError() != null)
            throw new Exception(progress.getError());

        return count;
    }

    /**
     * Reads the header row of the CSV to populate {@code variantsAndPositions}.
     * All markers are unplaced (DArTag provides no position information).
     */
    private void readMarkerIds(URL fileURL, LinkedHashMap<String, String> variantsAndPositions) throws Exception {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileURL.openStream()))) {
            String headerLine = br.readLine();
            if (headerLine == null)
                throw new Exception("DArTag file is empty");

            String[] headers = headerLine.split(",", -1);
            if (headers.length <= FIRST_MARKER_COL)
                throw new Exception("DArTag header has no marker columns: " + headerLine);
            if (!headers[SUBJECT_ID_COL].trim().equals("SUBJECT_ID"))
                throw new Exception("Expected 'SUBJECT_ID' at column " + (SUBJECT_ID_COL + 1) + " but found '" + headers[SUBJECT_ID_COL].trim() + "'");

            for (int c = FIRST_MARKER_COL; c < headers.length; c++) {
                String markerId = headers[c].trim();
                if (markerId.isEmpty())
                    continue;
                if (variantsAndPositions.containsKey(markerId))
                    throw new Exception("Duplicate marker ID in header: " + markerId);
                variantsAndPositions.put(markerId, "0\t0");    // unplaced
            }
        }
    }

    /**
     * Transposes the sample-oriented genotype matrix into the marker-oriented
     * temporary TSV file expected by
     * {@link RefactoredImport#importTempFileContents}.
     *
     * <p>The file is re-read once per chunk of {@link #MARKER_CHUNK_SIZE}
     * markers. During each pass only {@code chunkSize} {@link StringBuilder}s
     * are held in memory (one per marker of the chunk, accumulating one
     * tab-separated genotype entry per sample). This bounds peak memory use to
     * {@code O(chunkSize × sampleCount)} regardless of total marker count.</p>
     *
     * <p>Genotypes are normalised from colon-separated ({@code "A:G"}) to
     * slash-separated ({@code "A/G"}) as required by
     * {@link RefactoredImport#importTempFileContents}. Single-character
     * homozygous genotypes ({@code "A:A"}) are collapsed to the single allele
     * form ({@code "A"}) so that {@link RefactoredImport}'s ploidy-expansion
     * logic handles them correctly. Empty cells and {@code "-"} are written as
     * empty strings (missing data).</p>
     *
     * <p>Sample IDs are collected during the first chunk pass and appended to
     * {@code individualListToFill}.</p>
     */
    private void transposeGenotypeFile(URL fileURL, LinkedHashMap<String, String> variantsAndPositions, File outputFile, Map<String, Type> nonSnpVariantTypeMapToFill, ArrayList<String> individualListToFill, ProgressIndicator progress) throws Exception {
        long before = System.currentTimeMillis();

        String[] markerIds = variantsAndPositions.keySet().toArray(new String[variantsAndPositions.size()]);
        int markerCount = markerIds.length;

        FileWriter outputWriter = new FileWriter(outputFile);
        try {
            for (int chunkStart = 0; chunkStart < markerCount; chunkStart += MARKER_CHUNK_SIZE) {
                int chunkSize = Math.min(MARKER_CHUNK_SIZE, markerCount - chunkStart);
                boolean fFirstChunk = chunkStart == 0;

                StringBuilder[] transposed = new StringBuilder[chunkSize];
                for (int i = 0; i < chunkSize; i++)
                    transposed[i] = new StringBuilder();

                @SuppressWarnings("unchecked")
                java.util.Set<String>[] distinctAlleles = new java.util.LinkedHashSet[chunkSize];
                for (int i = 0; i < chunkSize; i++)
                    distinctAlleles[i] = new java.util.LinkedHashSet<>();

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fileURL.openStream()))) {
                    br.readLine();    // skip header

                    int lineNumber = 1;
                    String line;
                    while ((line = br.readLine()) != null) {
                        lineNumber++;
                        if (line.trim().isEmpty())
                            continue;

                        String[] fields = line.split(",", -1);
                        String sampleId = fields.length > SUBJECT_ID_COL ? fields[SUBJECT_ID_COL].trim() : "";
                        if (sampleId.isEmpty()) {
                            LOG.warn("Skipping row " + lineNumber + ": empty SUBJECT_ID");
                            continue;
                        }

                        if (fFirstChunk)
                            individualListToFill.add(sampleId);

                        for (int i = 0; i < chunkSize; i++) {
                            int col = FIRST_MARKER_COL + chunkStart + i;
                            String raw = col < fields.length ? fields[col].trim() : "";
                            String normalised = normaliseGenotype(raw);
                            transposed[i].append("\t");
                            if (normalised != null) {
                                transposed[i].append(normalised);
                                for (String allele : normalised.split("/"))
                                    distinctAlleles[i].add(allele);
                            }
                        }
                    }
                }

                // Write this chunk's marker lines to the output file and detect non-SNP types
                for (int i = 0; i < chunkSize; i++) {
                    String variantName = markerIds[chunkStart + i];

                    if (!distinctAlleles[i].isEmpty()) {
                        Type variantType = determineType(distinctAlleles[i].stream().map(a -> Allele.create(a)).collect(Collectors.toList()));
                        if (variantType != Type.SNP)
                            nonSnpVariantTypeMapToFill.put(variantName, variantType);
                    }

                    // skip leading tab from the StringBuilder
                    String variantLine = transposed[i].length() > 0 ? transposed[i].substring(1) : "";
                    outputWriter.write(variantName);
                    outputWriter.write("\t");
                    outputWriter.write(variantLine);
                    outputWriter.write("\n");
                }

                progress.setCurrentStepProgress((chunkStart + chunkSize) * 100 / markerCount);
            }
        } finally {
            outputWriter.close();
        }

        LOG.info("DArTag transposition took " + (System.currentTimeMillis() - before) + "ms for " + markerCount + " markers and " + individualListToFill.size() + " samples");
        Runtime.getRuntime().gc();
    }

    /**
     * Normalises a raw DArTag genotype cell value into the slash-separated
     * format expected by {@link RefactoredImport#importTempFileContents}.
     *
     * <ul>
     *   <li>{@code "A:G"} → {@code "A/G"} (heterozygote)</li>
     *   <li>{@code "A:A"} → {@code "A"}   (collapsed homozygote, let RefactoredImport expand it)</li>
     *   <li>empty string or {@code "-"}    → {@code null} (missing data)</li>
     * </ul>
     */
    static String normaliseGenotype(String raw) {
        if (raw == null || raw.isEmpty() || "-".equals(raw)) {
            return null;
        }

        String[] parts = raw.split(":");
        if (parts.length == 1) {
            // No colon: must be a single allele (e.g., "A")
            String allele = parts[0].trim().toUpperCase();
            if (allele.isEmpty() || !allele.matches("[ATGC]")) {
                if (!allele.isEmpty()) {
                    LOG.warn("Unrecognised DArTag genotype value: '" + raw + "' — treated as missing");
                }
                return null;
            }
            return allele; // RefactoredImport will expand to allele/allele using ploidy
        }

        if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
            LOG.warn("Unrecognised DArTag genotype value: '" + raw + "' — treated as missing");
            return null;
        }

        String a1 = parts[0].trim().toUpperCase();
        String a2 = parts[1].trim().toUpperCase();

        // Check if both alleles are valid (A, T, G, or C)
        if (!a1.matches("[ATGC]") || !a2.matches("[ATGC]")) {
            LOG.warn("Unrecognised DArTag genotype value: '" + raw + "' — treated as missing");
            return null;
        }

        if (a1.equals(a2)) {
            return a1; // collapsed homozygote: RefactoredImport will expand to a1/a1 using ploidy
        } else {
            return a1 + "/" + a2;
        }
    }
}