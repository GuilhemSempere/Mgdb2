package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "customSampleMetadata")
@TypeAlias("SM")
public class CustomSampleMetadata {
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	static public class CustomSampleMetadataId {
                
         /** The Constant FIELDNAME_SAMPLE_ID. */
		public final static String FIELDNAME_SAMPLE_ID= "si";
		
		/** The Constant FIELDNAME_USER. */
		public final static String FIELDNAME_USER = "ur";
                
		/** The sample id. */
		@Field(FIELDNAME_SAMPLE_ID)
		private Integer sampleId;

		/** The user name. */
		@Field(FIELDNAME_USER)
		private String user;


		/**
		 * Instantiates a new custom sample metadata id.
		 *
		 * @param sampleId the sample id
		 * @param user the user's name
		 */
		public CustomSampleMetadataId(Integer sampleId, String user) {
			this.sampleId = sampleId;
			this.user = user;
		}

		/**
		 * Gets the run name.
		 *
		 * @return the run name
		 */
		public String getUser() {
			return user;
		}

        public Integer getSampleId() {
            return sampleId;
        }  
	}
	
	
	/** The id. */
	@Id
	private CustomSampleMetadataId id;

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private LinkedHashMap<String, Object> additionalInfo = null;
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public CustomSampleMetadataId getId() {
		return id;
	}
	
	
	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Object> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new LinkedHashMap<String, Object>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(LinkedHashMap<String, Object> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
	
	@Override
	public String toString() {
		return id.getSampleId() + "§" + id.getUser();
	}

}