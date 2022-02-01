package fr.cirad.mgdb.annotation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.snpeff.interval.Chromosome;
import org.snpeff.interval.Genome;
import org.snpeff.interval.Variant;
import org.snpeff.snpEffect.Config;
import org.snpeff.snpEffect.SnpEffectPredictor;
import org.snpeff.snpEffect.VariantEffect;
import org.snpeff.snpEffect.VariantEffects;
import org.snpeff.vcf.EffFormatVersion;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;

import fr.cirad.mgdb.importing.VcfImport;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;


public class SnpEffAnnotationService {
    protected static final Logger LOG = Logger.getLogger(SnpEffAnnotationService.class);

	public static String annotateRun(String module, int projectId, String run, String snpEffDatabase, ProgressIndicator progress) {
		MongoTemplate template = MongoTemplateManager.get(module);

		progress.addStep("Loading config");
		progress.moveToNextStep();
		Config config = new Config(snpEffDatabase, "/home/u017-h433/Documents/deps/snpEff/snpEff.config", "/home/u017-h433/Documents/deps/snpEff/data", null);
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

	public static Chromosome getParentChromosome(VariantRunData vrd, Genome genome) {
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
}
