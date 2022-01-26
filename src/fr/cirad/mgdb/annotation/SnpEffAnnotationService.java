package fr.cirad.mgdb.annotation;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.snpeff.interval.Chromosome;
import org.snpeff.interval.Genome;
import org.snpeff.snpEffect.Config;
import org.snpeff.snpEffect.SnpEffectPredictor;
import org.snpeff.snpEffect.VariantEffect;
import org.snpeff.snpEffect.VariantEffects;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;

import fr.cirad.mgdb.importing.VcfImport;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;


public class SnpEffAnnotationService {
    protected static final Logger LOG = Logger.getLogger(SnpEffAnnotationService.class);

	public static String annotateRun(String module, int project, String run, String snpEffDatabase) {
		MongoTemplate template = MongoTemplateManager.get(module);

		LOG.info("Loading config");
		Config config = new Config(snpEffDatabase, "/home/u017-h433/Documents/deps/snpEff/snpEff.config", "/home/u017-h433/Documents/deps/snpEff/data", null);
		Genome genome = config.getGenome();
		SnpEffectPredictor predictor = config.loadSnpEffectPredictor();
		predictor.buildForest();
		LOG.info("Loaded genome " + genome.getGenomeId());

		BasicDBObject vrdQuery = new BasicDBObject();
		vrdQuery.put("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID, project);
		vrdQuery.put("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME, run);
		FindIterable<Document> variantRunData = template.getCollection(MongoTemplateManager.getMongoCollectionName(VariantRunData.class)).find(vrdQuery).batchSize(100);

		LOG.info("Processing variants");
		for (Document doc : variantRunData) {
			VariantRunData vrd = template.getConverter().read(VariantRunData.class, doc);
			Chromosome chromosome = getParentChromosome(vrd, genome);
			SnpEffVariantWrapper variant = new SnpEffVariantWrapper(chromosome, vrd, null);

			VariantRunData result = annotateVariant(variant, predictor);
			if (result != null)
				template.save(result);
		}

		LOG.info("Updating header");

		BasicDBObject queryVarAnn = new BasicDBObject();
        queryVarAnn.put("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_PROJECT, project);
        queryVarAnn.put("_id." + DBVCFHeader.VcfHeaderId.FIELDNAME_RUN, run);

        DBVCFHeader header = null;
        Document headerDoc = template.getCollection(MongoTemplateManager.getMongoCollectionName(DBVCFHeader.class)).find(queryVarAnn).first();
        if (headerDoc == null) {
        	header = new DBVCFHeader(new VcfHeaderId(project, run), false, false, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
        } else {
        	header = DBVCFHeader.fromDocument(headerDoc);
        }

        VCFInfoHeaderLine headerLine = new VCFInfoHeaderLine(VcfImport.ANNOTATION_FIELDNAME_ANN, VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.String, "Predicted effects for this variant");
        header.getmInfoMetaData().put(VcfImport.ANNOTATION_FIELDNAME_ANN, headerLine);

        template.save(header);

		return null;
	}

	private static VariantRunData annotateVariant(SnpEffVariantWrapper variant, SnpEffectPredictor predictor) {
		// Calculate effects: By default do not annotate non-variant sites
		if (!variant.isVariant()) return null;

		/*boolean impactModerateOrHigh = false; // Does this entry have a 'MODERATE' or 'HIGH' impact?
		boolean impactLowOrHigher = false; // Does this entry have an impact (other than MODIFIER)?*/

		VariantEffects variantEffects = predictor.variantEffect(variant);

		// Show results
		for (VariantEffect variantEffect : variantEffects) {
			/*if (variantEffect.hasError()) errByType.inc(variantEffect.getError());
			if (variantEffect.hasWarning()) warnByType.inc(variantEffect.getWarning());*/

			/*// Does this entry have an impact (other than MODIFIER)?
			impactLowOrHigher |= (variantEffect.getEffectImpact() != EffectImpact.MODIFIER);
			impactModerateOrHigh |= (variantEffect.getEffectImpact() == EffectImpact.MODERATE) || (variantEffect.getEffectImpact() == EffectImpact.HIGH);*/
			variant.addEffect(variantEffect);
		}
		return variant.buildANN();
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
