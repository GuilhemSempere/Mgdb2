package fr.cirad.mgdb.annotation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.snpeff.SnpEff;
import org.snpeff.interval.Chromosome;
import org.snpeff.interval.Genome;
import org.snpeff.interval.Variant;
import org.snpeff.snpEffect.Config;
import org.snpeff.snpEffect.SnpEffectPredictor;
import org.snpeff.snpEffect.VariantEffect;
import org.snpeff.snpEffect.VariantEffects;
import org.snpeff.util.Download;
import org.snpeff.util.Log;
import org.snpeff.vcf.EffFormatVersion;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;

import fr.cirad.mgdb.importing.VcfImport;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;


/**
 * Service that interfaces Mgdb2 and SnpEff to annotate variants directly from an Mgdb2 database
 * @author Grégori MIGNEROT
 * TODO : Parallelize annotation
 */
public class SnpEffAnnotationService {
    protected static final Logger LOG = Logger.getLogger(SnpEffAnnotationService.class);

    public static final String SNPEFF_CONFIG_START_LINE = "##{{MGDB_BLOCK:";
    public static final String SNPEFF_CONFIG_END_LINE = "##:END}}";

    // By defaults, SnpEff fatal errors call System.exit, which we do *NOT* want
    static {
    	Log.setFatalErrorBehabiour(Log.FatalErrorBehabiour.EXCEPTION);
    }

	public static String annotateRun(String configFile, String dataPath, String module, int projectId, String run, String snpEffDatabase, ProgressIndicator progress) {
		MongoTemplate template = MongoTemplateManager.get(module);

		progress.addStep("Loading config");
		progress.moveToNextStep();
		Config config = new Config(snpEffDatabase, configFile, dataPath, null);
		Genome genome = config.getGenome();
		SnpEffectPredictor predictor = config.loadSnpEffectPredictor();
		predictor.buildForest();
		LOG.info("Loaded genome " + genome.getGenomeId());

		GenotypingProject project = template.findById(projectId, GenotypingProject.class);
		TreeSet<String> projectEffects = project.getEffectAnnotations();

		BasicDBObject vrdQuery = new BasicDBObject();
		vrdQuery.put("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID, projectId);
		vrdQuery.put("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME, run);
		FindIterable<Document> variantRunData = template.getCollection(MongoTemplateManager.getMongoCollectionName(VariantRunData.class)).find(vrdQuery).batchSize(100);

		progress.addStep("Processing variants");
		progress.moveToNextStep();
		int processedVariants = 0;
		for (Document doc : variantRunData) {
			VariantRunData vrd = template.getConverter().read(VariantRunData.class, doc);
			Chromosome chromosome = getParentChromosome(vrd, genome);
			SnpEffEntryWrapper entry = new SnpEffEntryWrapper(chromosome, vrd);

			VariantRunData result = annotateVariant(entry, predictor, projectEffects);
			if (result != null) {
				template.save(result);
				processedVariants += 1;
				progress.setCurrentStepProgress(processedVariants);
			} else {
				LOG.warn("Failed to annotate variant " + vrd.getId());
			}
		}

		progress.addStep("Updating project metadata");
		progress.moveToNextStep();

		BasicDBObject queryVarAnn = new BasicDBObject();
        queryVarAnn.put("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_PROJECT, projectId);
        queryVarAnn.put("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_RUN, run);

        DBVCFHeader header = null;
        Document headerDoc = template.getCollection(MongoTemplateManager.getMongoCollectionName(DBVCFHeader.class)).find(queryVarAnn).first();
        if (headerDoc == null) {
        	header = new DBVCFHeader(new VcfHeaderId(projectId, run), false, false, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
        } else {
        	header = DBVCFHeader.fromDocument(headerDoc);
        }

        // FIXME : Retrieve this from EffFormatVersion.FORMAT_ANN_1.vcfHeader()
        String description = "Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'";
        VCFInfoHeaderLine headerLine = new VCFInfoHeaderLine(VcfImport.ANNOTATION_FIELDNAME_ANN, VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.String, description);
        header.getmInfoMetaData().put(VcfImport.ANNOTATION_FIELDNAME_ANN, headerLine);
        template.save(header);

        template.save(project);

		return null;
	}

	private static VariantRunData annotateVariant(SnpEffEntryWrapper entry, SnpEffectPredictor predictor, Set<String> effectAnnotations) {
		for (Variant variant : entry.variants()) {
			// Calculate effects: By default do not annotate non-variant sites
			if (!variant.isVariant()) return null;

			VariantEffects variantEffects = predictor.variantEffect(variant);

			// Show results
			for (VariantEffect variantEffect : variantEffects) {
				entry.addEffect(variantEffect);
				effectAnnotations.add(variantEffect.getEffectTypeString(true, false, EffFormatVersion.FORMAT_ANN_1));
			}
		}

		return entry.buildANN();
	}

	private static Chromosome getParentChromosome(VariantRunData vrd, Genome genome) {
		String sequence = vrd.getReferencePosition().getSequence();
		Chromosome chromosome = genome.getChromosome(sequence);
		if (chromosome != null) return chromosome;

		// Isolate the numeric part
		Pattern singleNumericPattern = Pattern.compile("\\D*(\\d+)\\D*");
		Matcher matcher = singleNumericPattern.matcher(sequence);
		matcher.find();
		if (matcher.matches()) {
			chromosome = genome.getChromosome(matcher.group(1));
			if (chromosome != null) return chromosome;
		}

		throw new RuntimeException("Chromosome name " + sequence + " is not compatible with the SnpEff database");
	}

	public static List<String> getAvailableGenomes(String configFile, String dataPath) {
    	File repository = new File(dataPath);
    	File[] databases = repository.listFiles(File::isDirectory);
    	List<String> availableGenomes = Arrays.stream(databases).map(file -> file.getName()).filter(fileName -> !fileName.equals("genomes")).collect(Collectors.toList());
    	return availableGenomes;
	}

	public static List<String> getDownloadableGenomes(String configFile, String dataPath) {
		Config config = new Config("", configFile, dataPath, null);
		List<String> downloadableGenomes = new ArrayList<>();
		for (String genome : config)
			downloadableGenomes.add(genome);
		return downloadableGenomes;
	}

	public static void downloadGenome(String configFile, String dataPath, String genomeVersion, ProgressIndicator progress) throws Exception {
		if (genomeVersion == "snpEff")
			throw new IllegalArgumentException("This command is not allowed");

		progress.addStep("Installing the genome");
		progress.moveToNextStep();

		Config config = new Config("", configFile, dataPath, null);

        List<URL> urls = config.downloadUrl(genomeVersion);
        if (urls.isEmpty()) {
        	progress.setError("Unknown genome, you must give the URL or base files");
        	return;
        }

        boolean maskExceptions = (urls.size() > 1);
        for (URL url : urls) {
        	if (downloadGenome(config, url, maskExceptions))
        		return;
        }

        progress.setError("Genome download failed");
	}

	public static void downloadGenome(String configFile, String dataPath, URL genomeURL, ProgressIndicator progress) throws Exception {
		progress.addStep("Installing the genome");
		progress.moveToNextStep();

		Config config = new Config("", configFile, dataPath, null);

        if (!downloadGenome(config, genomeURL, false))
        	progress.setError("Genome download failed");
	}

	private static boolean downloadGenome(Config config, URL url, boolean maskExceptions) {
		String localFile = System.getProperty("java.io.tmpdir") + "/" + Download.urlBaseName(url.toString());

        Download download = new Download();
        download.setVerbose(false);
        download.setDebug(false);
        download.setUpdate(false);
        download.setMaskDownloadException(maskExceptions);
        LOG.info("Try to download genome from " + url.toString());
        if (download.download(url, localFile)) {
        	LOG.debug("Unzipping " + localFile);
            if (download.unzip(localFile, config.getDirMain(), config.getDirData())) {
                (new File(localFile)).delete();
                LOG.info("Installation successful");
                return true;
            }
        }

        return false;
	}

	public static String importGenome(String genomeID, String genomeName, File sequenceFile, File referenceFile, File cdsFile, File proteinFile, String referenceFormat, String configPath, String dataPath, ProgressIndicator progress) throws IOException {
		progress.addStep("Checking config");
		progress.moveToNextStep();
		String configContent = new String(Files.readAllBytes(Paths.get(configPath)));
		for (String line : configContent.split("\n")) {
			if (!line.isBlank() && !line.startsWith("#")) {
				String property;
				if (line.contains(":"))
					property = line.substring(0, line.indexOf(':')).trim();
				else if (line.contains("="))
					property = line.substring(0, line.indexOf('=')).trim();
				else continue;

				if (property.startsWith(genomeID + ".") || property.equals(genomeID)) {
					progress.setError("This genome name already exists or conflicts with an existing genome");
					return null;
				}
			}
		}

		progress.addStep("Writing config");
		progress.moveToNextStep();

		StringBuilder genomeConfig = new StringBuilder();
		genomeConfig.append(SNPEFF_CONFIG_START_LINE + "\n");
		genomeConfig.append(genomeID + ".genome : " + ((genomeName == null || genomeName.isEmpty()) ? genomeID : genomeName) + "\n");
		genomeConfig.append(SNPEFF_CONFIG_END_LINE + "\n");

		FileWriter configWriter = new FileWriter(configPath, true);
		configWriter.write(genomeConfig.toString());
		configWriter.close();

		progress.addStep("Setting up the database");
		progress.moveToNextStep();

		Path genomePath = Paths.get(dataPath, genomeID);
		File genomeDir = genomePath.toFile();
		genomeDir.mkdirs();

		Files.move(sequenceFile.toPath(), genomePath.resolve("sequences.fa"));
		if (referenceFormat.equals("gtf22")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.gtf"));
		} else if (referenceFormat.equals("gff2")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.gff2"));
		} else if (referenceFormat.equals("gff3")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.gff"));
		} else if (referenceFormat.equals("refSeq")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.refseq"));
		} else if (referenceFormat.equals("genbank")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.gbk"));
		} else if (referenceFormat.equals("knowngenes")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.kg"));
		} else if (referenceFormat.equals("embl")) {
			Files.move(referenceFile.toPath(), genomePath.resolve("genes.embl"));
		} else {
			progress.setError("Unsupported reference file format");
			return null;
		}

		if (cdsFile != null)
			Files.move(cdsFile.toPath(), genomePath.resolve("cds.fa"));
		if (proteinFile != null)
			Files.move(proteinFile.toPath(), genomePath.resolve("protein.fa"));

		ByteArrayOutputStream snpEffOutput = new ByteArrayOutputStream();
		System.setOut(new PrintStream(snpEffOutput, true, StandardCharsets.UTF_8));
		System.setErr(new PrintStream(snpEffOutput, true, StandardCharsets.UTF_8));
		try {
			List<String> args = new ArrayList<>();
			args.add("build");
			args.add("-c"); args.add(configPath);
			args.add("-dataDir"); args.add(dataPath);
			args.add("-" + referenceFormat);
			if (cdsFile == null)
				args.add("-noCheckCds");
			if (proteinFile == null)
				args.add("-noCheckProtein");
			args.add(genomeID);

			progress.addStep("Building the database");
			progress.moveToNextStep();

			SnpEff command = new SnpEff(args.toArray(new String[args.size()]));
			if (!command.run()) {
				progress.setError("Database building failed");
				LOG.error("Database building failed");

				sweepFailedInstall(configPath, genomeConfig.toString(), genomeDir);
			}
		} catch (Exception exc) {
			progress.setError("Error while building the database : " + exc.getMessage());
			LOG.error("Error while building the database : ", exc);

			sweepFailedInstall(configPath, genomeConfig.toString(), genomeDir);
		} finally {
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
		}
		return snpEffOutput.toString(StandardCharsets.UTF_8);
	}

	private static boolean deleteDirectory(File directoryToBeDeleted) {
	    File[] allContents = directoryToBeDeleted.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	            deleteDirectory(file);
	        }
	    }
	    return directoryToBeDeleted.delete();
	}

	// Clear the config file of the changes we made earlier and remove the genome
	private static void sweepFailedInstall(String configPath, String genomeConfig, File genomeDir) throws IOException {
		String configContent = new String(Files.readAllBytes(Paths.get(configPath)));
		int genomeConfigPosition = configContent.indexOf(genomeConfig.toString());
		if (genomeConfigPosition >= 0) {
			configContent = configContent.substring(0, genomeConfigPosition).concat(configContent.substring(genomeConfigPosition + genomeConfig.length()));
			FileWriter configWriter = new FileWriter(configPath, false);
			configWriter.write(configContent);
			configWriter.close();
		} else {
			LOG.warn("Config lines that have just been written are not found anymore");
		}

		deleteDirectory(genomeDir);
	}
}
