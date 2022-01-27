package fr.cirad.mgdb.annotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.snpeff.interval.Marker;
import org.snpeff.interval.Variant;
import org.snpeff.snpEffect.VariantEffect;
import org.snpeff.vcf.EffFormatVersion;
import org.snpeff.vcf.VcfEffect;

import fr.cirad.mgdb.importing.VcfImport;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;


/**
 * Implementation of org.snpEff.interval.Variant's interface around an Mgdb2 variant
 * @author Grégori MIGNEROT
 */
public class SnpEffVariantWrapper extends Variant {
	private static final long serialVersionUID = 4989761070007311042L;

	private VariantRunData variant;
	private DBVCFHeader header;

	private List<VariantEffect> effects;

	public SnpEffVariantWrapper(Marker parent, VariantRunData vrd, DBVCFHeader header) {
		super(parent,
				(int)vrd.getReferencePosition().getStartSite(),
				vrd.getKnownAlleleList().get(0),
				String.join(",", vrd.getKnownAlleleList().subList(1, vrd.getKnownAlleleList().size())),
				vrd.getId().getVariantId());

		this.variant = vrd;
		this.header = header;
		this.effects = new ArrayList<VariantEffect>();
	}

	public void addEffect(VariantEffect effect) {
		this.effects.add(effect);
	}

	public VariantRunData buildANN() {
		StringBuilder annotation = new StringBuilder();
		ArrayList<String> effectNames = new ArrayList<>();
		ArrayList<String> effectGenes = new ArrayList<>();

		// FIXME : Some parts are missing or incomplete, check VcfEffect
		for (VariantEffect effect : effects) {
			VcfEffect vcfEffect = new VcfEffect(effect, EffFormatVersion.FORMAT_ANN_1);
			annotation.append(vcfEffect.toString()).append(",");
			effectNames.add(vcfEffect.getEffectsStr());
			effectGenes.add(vcfEffect.getGeneId());
		}
		annotation.setLength(annotation.length() - 1);  // Trim the last comma

		HashMap<String, Object> info = variant.getAdditionalInfo();
		info.put(VcfImport.ANNOTATION_FIELDNAME_ANN, annotation.toString());
		info.put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, effectNames);
		info.put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, effectGenes);

		return variant;
	}
}
