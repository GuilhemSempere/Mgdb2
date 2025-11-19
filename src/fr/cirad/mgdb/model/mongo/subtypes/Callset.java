package fr.cirad.mgdb.model.mongo.subtypes;

import javax.ejb.ObjectNotFoundException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;

@Document(collection = "callsets")
//@TypeAlias("CS")
public class Callset {
    public final static String FIELDNAME_PROJECT_ID = "pj";
    public final static String FIELDNAME_RUN = "rn";

    /** The callset id. */
    @Id
    private int id;

    /** The sample id. */
    @Transient
    private String sampleId;

    /** The individual. */
    @Transient
    private String individual;

    /** The projectId. */
    @Field(FIELDNAME_PROJECT_ID)
    private int projectId;

    /** The run. */
    @Field(FIELDNAME_RUN)
    private String run;

    public Callset() {
    }

    public Callset(int id, GenotypingSample sample, int projectId, String run) {
        this.id = id;
        this.sampleId = sample.getId();
        this.individual = sample.getIndividual();
        this.projectId = projectId;
        this.run = run;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
   
	public void setSampleId(String sampleId) {
		this.sampleId = sampleId;
	}

	public void setIndividual(String individual) {
		this.individual = individual;
	}

	public String getSampleId() throws ObjectNotFoundException {
		if (sampleId == null)
			throw new ObjectNotFoundException("If you load directly CallSet objects from the GenotypingSample's collection then their sample reference is not set");
		return sampleId;
	}
	
    public String getIndividual() throws ObjectNotFoundException {
		if (individual == null)
			throw new ObjectNotFoundException("If you load directly CallSet objects from the GenotypingSample's collection then their sample reference is not set");
    	return individual;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getRun() {
        return run;
    }

    public void setRun(String run) {
        this.run = run;
    }
}
