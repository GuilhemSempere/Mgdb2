package fr.cirad.mgdb.model.mongo.maintypes;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "callsets")
@TypeAlias("CS")
public class CallSet {

    public final static String FIELDNAME_SAMPLE = "sp";
    public final static String FIELDNAME_INDIVIDUAL = "in";
    public final static String FIELDNAME_PROJECT_ID = "pj";
    public final static String FIELDNAME_RUN = "rn";

    /** The callset id. */
    @Id
    private int id;

    /** The sample id. */
    @Field(FIELDNAME_SAMPLE)
    private String sampleId;

    /** The individual. */
    @Field(FIELDNAME_INDIVIDUAL)
    private String individual;

    /** The projectId. */
    @Field(FIELDNAME_PROJECT_ID)
    private int projectId;

    /** The run. */
    @Field(FIELDNAME_RUN)
    private String run;

    public CallSet(int id, String sampleId, String individual, int projectId, String run) {
        this.id = id;
        this.sampleId = sampleId;
        this.individual = individual;
        this.projectId = projectId;
        this.run = run;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }

    public String getIndividual() {
        return individual;
    }

    public void setIndividual(String individual) {
        this.individual = individual;
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
