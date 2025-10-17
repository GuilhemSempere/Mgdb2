package fr.cirad.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.mapping.event.*;
import org.springframework.stereotype.Component;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;

@Component
public class SampleAfterConvertListener extends AbstractMongoEventListener<GenotypingSample> {

    private static final Logger log = LoggerFactory.getLogger(SampleAfterConvertListener.class);

    public SampleAfterConvertListener() {
        log.info("SampleAfterConvertListener CONSTRUCTOR CALLED - bean is being created");
    }
    
    @Override
    public void onAfterConvert(AfterConvertEvent<GenotypingSample> event) {
    	GenotypingSample sample = event.getSource();
        for (Callset cs : sample.getCallSets()) {
            cs.setSampleId(sample.getId());
        	cs.setIndividual(sample.getIndividual());
        }
    }
}
