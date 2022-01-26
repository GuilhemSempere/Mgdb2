package fr.cirad.mgdb.annotation;

import org.snpeff.interval.Marker;
import org.snpeff.interval.Variant;
import org.snpeff.snpEffect.VariantEffect;

import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;


/**
 * Implementation of org.snpEff.interval.Variant's interface around an Mgdb2 variant
 * @author Grégori MIGNEROT
 * @implNote As this makes use of undocumented code that is not supposed to be used as a library, this deliberately contains lots of internal encapsulation to minimize the effort to keep it up to date, even if it harms performance a little bit
 */
public class SnpEffVariantWrapper extends Variant {
	private static final long serialVersionUID = 4989761070007311042L;

	private VariantRunData variant;
	private DBVCFHeader header;

	public SnpEffVariantWrapper(Marker parent, VariantRunData vrd, DBVCFHeader header) {
		super(parent,
				(int)vrd.getReferencePosition().getStartSite(),
				vrd.getKnownAlleleList().get(0),
				String.join(",", vrd.getKnownAlleleList().subList(1, vrd.getKnownAlleleList().size())),
				vrd.getId().getVariantId());

		this.variant = vrd;
		this.header = header;
	}

	public void addEffect(VariantEffect effect) {
		System.out.println(effect.toString());
	}
}
