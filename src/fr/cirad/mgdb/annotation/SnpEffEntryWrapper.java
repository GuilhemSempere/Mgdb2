package fr.cirad.mgdb.annotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.snpeff.interval.Chromosome;
import org.snpeff.interval.Variant;
import org.snpeff.snpEffect.VariantEffect;
import org.snpeff.vcf.EffFormatVersion;
import org.snpeff.vcf.VcfEffect;

import fr.cirad.mgdb.importing.VcfImport;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;

/**
 * Wrapper around a VariantRunData entry to act like a SnpEff's VcfEntry,
 * that can be split into multiple variants, one for each alternative allele
 * @author Grégori MIGNEROT
 */
public class SnpEffEntryWrapper {
	private Chromosome parent;
	private VariantRunData entry;
	private List<VariantEffect> effects;

	public SnpEffEntryWrapper(Chromosome parent, VariantRunData entry) {
		this.parent = parent;
		this.entry = entry;
		this.effects = new ArrayList<VariantEffect>();
	}

	public void addEffect(VariantEffect effect) {
		this.effects.add(effect);
	}

	public List<Variant> variants() {
		return Variant.factory(parent, (int)entry.getReferencePosition().getStartSite(), entry.getKnownAlleleList().get(0),
				String.join(",", entry.getKnownAlleleList().subList(1, entry.getKnownAlleleList().size())), entry.getVariantId(), true);
	}

	public VariantRunData buildANN() {
		StringBuilder annotation = new StringBuilder();
		ArrayList<String> effectNames = new ArrayList<>();
		ArrayList<String> effectGenes = new ArrayList<>();

		for (VariantEffect effect : effects) {
			VcfEffect vcfEffect = new VcfEffect(effect, EffFormatVersion.FORMAT_ANN_1);
			annotation.append(vcfEffect.toString()).append(",");
			effectNames.add(vcfEffect.getEffectTypesStr());
			effectGenes.add(vcfEffect.getGeneId());
		}
		annotation.setLength(annotation.length() - 1);  // Trim the last comma

		HashMap<String, Object> info = entry.getAdditionalInfo();
		info.put(VcfImport.ANNOTATION_FIELDNAME_ANN, annotation.toString());
		info.put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, effectNames);
		info.put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, effectGenes);

		return entry;
	}
}
