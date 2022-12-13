/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.mgdb.model.mongo.maintypes;

import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFSimpleHeaderLine;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * The Class DBVCFHeader.
 */
@Document(collection = "vcf_header")
@TypeAlias("VH")
public class DBVCFHeader
{	
	
	/**
	 * The Class VcfHeaderId.
	 */
	static public class VcfHeaderId
	{
		
		/** The Constant FIELDNAME_PROJECT. */
		public final static String FIELDNAME_PROJECT = "pj";
		
		/** The Constant FIELDNAME_RUN. */
		public final static String FIELDNAME_RUN = "rn";

		/** The project. */
		@Field(FIELDNAME_PROJECT)
		private Integer project;
		
		/** The run. */
		@Field(FIELDNAME_RUN)
		private String run;

		/**
		 * Instantiates a new vcf header id.
		 */
		public VcfHeaderId() {
		}

		/**
		 * Instantiates a new vcf header id.
		 *
		 * @param project the project
		 * @param run the run
		 */
		public VcfHeaderId(Integer project, String run) {
			this.project = project;
			this.run = run;
		}

                public Integer getProject() {
                    return project;
                }

                public String getRun() {
                    return run;
                }
                
	}
	
	/** The id. */
	private VcfHeaderId id;
	
	/** The write command line. */
	private boolean writeCommandLine;
	
	/** The write engine headers. */
	private boolean writeEngineHeaders; 
	
	/** The m info meta data. */
	private Map<String, VCFInfoHeaderLine> mInfoMetaData = new LinkedHashMap();
	
	/** The m format meta data. */
        public final static String FIELDNAME_FORMAT_METADATA = "mFormatMetaData";

	private Map<String, VCFFormatHeaderLine> mFormatMetaData = new LinkedHashMap();
	
	/** The m filter meta data. */
	private Map<String, VCFFilterHeaderLine> mFilterMetaData = new LinkedHashMap();
	
	/** The m other meta data. */
	private Map<String, VCFHeaderLine> mOtherMetaData = new LinkedHashMap();
	
	/** The m meta data. */
	private Map<String, VCFSimpleHeaderLine> mMetaData = new LinkedHashMap();

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(DBVCFHeader.class);
	
	/**
	 * Instantiates a new DBVCF header.
	 */
	public DBVCFHeader()
	{
	}
	
	/**
	 * Instantiates a new DBVCF header.
	 *
	 * @param id the id
	 * @param writeCommandLine the write command line
	 * @param writeEngineHeaders the write engine headers
	 * @param mInfoMetaData the m info meta data
	 * @param mFormatMetaData the m format meta data
	 * @param mFilterMetaData the m filter meta data
	 * @param mOtherMetaData the m other meta data
	 * @param mMetaData the m meta data
	 */
	public DBVCFHeader(VcfHeaderId id, Boolean writeCommandLine, Boolean writeEngineHeaders, Map<String, VCFInfoHeaderLine> mInfoMetaData, Map<String, VCFFormatHeaderLine> mFormatMetaData, Map<String, VCFFilterHeaderLine> mFilterMetaData, Map<String, VCFHeaderLine> mOtherMetaData, Map<String, VCFSimpleHeaderLine> mMetaData)
	{
		super();
		this.id = id;
		this.writeCommandLine = writeCommandLine;
		this.writeEngineHeaders = writeEngineHeaders;
		this.mInfoMetaData = mInfoMetaData;
		this.mFormatMetaData = mFormatMetaData;
		this.mFilterMetaData = mFilterMetaData;
		this.mOtherMetaData = mOtherMetaData;
		this.mMetaData = mMetaData;
	}

	/**
	 * Instantiates a new DBVCF header.
	 *
	 * @param id the id
	 * @param header the header
	 */
	public DBVCFHeader(VcfHeaderId id, VCFHeader header)
	{
		this.id = id;
		this.writeCommandLine = header.isWriteCommandLine();
		this.writeEngineHeaders = header.isWriteEngineHeaders();
		
		for (VCFHeaderLine line : header.getMetaDataInInputOrder())
		{
			if (VCFHeaderLine.class.equals(line.getClass()))
				mOtherMetaData.put(((VCFHeaderLine) line).getKey(), line);
			else if (VCFFormatHeaderLine.class.equals(line.getClass()))
				mFormatMetaData.put(((VCFFormatHeaderLine) line).getID(), (VCFFormatHeaderLine) line);
			else if (VCFInfoHeaderLine.class.equals(line.getClass()))
				mInfoMetaData.put(((VCFInfoHeaderLine) line).getID(), (VCFInfoHeaderLine) line);
			else if (VCFFilterHeaderLine.class.equals(line.getClass()))
				mFilterMetaData.put(((VCFFilterHeaderLine) line).getID(), (VCFFilterHeaderLine) line);
			else if (VCFSimpleHeaderLine.class.equals(line.getClass()))
				mMetaData.put(((VCFSimpleHeaderLine) line).getKey(), (VCFSimpleHeaderLine) line);
		}
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public VcfHeaderId getId() {
		return id;
	}
	
	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(VcfHeaderId id) {
		this.id = id;
	}	
	
	/**
	 * Gets the write command line.
	 *
	 * @return the write command line
	 */
	public boolean getWriteCommandLine() {
		return writeCommandLine;
	}

	/**
	 * Sets the write command line.
	 *
	 * @param writeCommandLine the new write command line
	 */
	public void setWriteCommandLine(boolean writeCommandLine) {
		this.writeCommandLine = writeCommandLine;
	}

	/**
	 * Gets the write engine headers.
	 *
	 * @return the write engine headers
	 */
	public boolean getWriteEngineHeaders() {
		return writeEngineHeaders;
	}

	/**
	 * Sets the write engine headers.
	 *
	 * @param writeEngineHeaders the new write engine headers
	 */
	public void setWriteEngineHeaders(boolean writeEngineHeaders) {
		this.writeEngineHeaders = writeEngineHeaders;
	}

	/**
	 * Gets the m info meta data.
	 *
	 * @return the m info meta data
	 */
	public Map<String, VCFInfoHeaderLine> getmInfoMetaData() {
		return mInfoMetaData;
	}

	/**
	 * Setm info meta data.
	 *
	 * @param mInfoMetaData the m info meta data
	 */
	public void setmInfoMetaData(Map<String, VCFInfoHeaderLine> mInfoMetaData) {
		this.mInfoMetaData = mInfoMetaData;
	}

	/**
	 * Gets the m format meta data.
	 *
	 * @return the m format meta data
	 */
	public Map<String, VCFFormatHeaderLine> getmFormatMetaData() {
		return mFormatMetaData;
	}

	/**
	 * Setm format meta data.
	 *
	 * @param mFormatMetaData the m format meta data
	 */
	public void setmFormatMetaData(Map<String, VCFFormatHeaderLine> mFormatMetaData) {
		this.mFormatMetaData = mFormatMetaData;
	}

	/**
	 * Gets the m filter meta data.
	 *
	 * @return the m filter meta data
	 */
	public Map<String, VCFFilterHeaderLine> getmFilterMetaData() {
		return mFilterMetaData;
	}

	/**
	 * Setm filter meta data.
	 *
	 * @param mFilterMetaData the m filter meta data
	 */
	public void setmFilterMetaData(Map<String, VCFFilterHeaderLine> mFilterMetaData) {
		this.mFilterMetaData = mFilterMetaData;
	}

	/**
	 * Gets the m other meta data.
	 *
	 * @return the m other meta data
	 */
	public Map<String, VCFHeaderLine> getmOtherMetaData() {
		return mOtherMetaData;
	}

	/**
	 * Setm other meta data.
	 *
	 * @param mOtherMetaData the m other meta data
	 */
	public void setmOtherMetaData(Map<String, VCFHeaderLine> mOtherMetaData) {
		this.mOtherMetaData = mOtherMetaData;
	}

	/**
	 * Gets the m meta data.
	 *
	 * @return the m meta data
	 */
	public Map<String, VCFSimpleHeaderLine> getmMetaData() {
		return mMetaData;
	}

	/**
	 * Setm meta data.
	 *
	 * @param mMetaData the m meta data
	 */
	public void setmMetaData(Map<String, VCFSimpleHeaderLine> mMetaData) {
		this.mMetaData = mMetaData;
	}

	/**
	 * Gets the header lines.
	 *
	 * @return the header lines
	 */
	public Set<VCFHeaderLine> getHeaderLines()
	{
		Set<VCFHeaderLine> headerLines = new HashSet<VCFHeaderLine>();
		for (String key : mInfoMetaData.keySet())
			headerLines.add(mInfoMetaData.get(key));
		for (String key : mFormatMetaData.keySet())
			headerLines.add(mFormatMetaData.get(key));
		for (String key : mFilterMetaData.keySet())
			headerLines.add(mFilterMetaData.get(key));
		for (String key : mOtherMetaData.keySet())
			headerLines.add(mOtherMetaData.get(key));
		for (String key : mMetaData.keySet())
			headerLines.add(mMetaData.get(key));
		
		return headerLines;
	}
		
	/**
	 * From db object.
	 *
	 * @param document the db header
	 * @return the DBVCF header
	 */
	static public DBVCFHeader fromDocument(org.bson.Document document)
	{
		DBVCFHeader header = new DBVCFHeader();
		org.bson.Document idDbObj = (org.bson.Document) document.get("_id");
		header.setId(new VcfHeaderId((Integer) idDbObj.get(VcfHeaderId.FIELDNAME_PROJECT), (String) idDbObj.get(VcfHeaderId.FIELDNAME_RUN)));
		for (Object key : document.keySet())
		{
			Object val = document.get(key);
			//System.out.println("-----" + key + ": " + map.get(key).getClass().getName() + "------");
			if (java.lang.Boolean.class.equals(val.getClass()))
			{
				if ("writeCommandLine".equals(key))
					header.setWriteCommandLine((Boolean) val);
				else if ("writeEngineHeaders".equals(key))
					header.setWriteEngineHeaders((Boolean) val);
				else
					LOG.info("Unable to deal with boolean header attribute: " + key);
			}
			else if (org.bson.Document.class.equals(val.getClass()))
			{
				if ("mInfoMetaData".equals(key))
					for (String subKey : ((org.bson.Document) val).keySet())
					{
						org.bson.Document subVal = (org.bson.Document) ((org.bson.Document) val).get(subKey);
						VCFHeaderLineCount countType = VCFHeaderLineCount.valueOf(subVal.getString("countType"));
						if (countType.equals(VCFHeaderLineCount.INTEGER))
							header.getmInfoMetaData().put(subKey, new VCFInfoHeaderLine(subVal.getString("name"), subVal.getInteger("count"), VCFHeaderLineType.valueOf(subVal.getString("type")), subVal.getString("description")));
						else
							header.getmInfoMetaData().put(subKey, new VCFInfoHeaderLine(subVal.getString("name"), countType, VCFHeaderLineType.valueOf(subVal.getString("type")), subVal.getString("description")));
					}
				else if ("mFilterMetaData".equals(key))
					for (String subKey : ((org.bson.Document) val).keySet())
					{
						org.bson.Document subVal = (org.bson.Document) ((org.bson.Document) ((org.bson.Document) val).get(subKey)).get("genericFields");
						header.getmFilterMetaData().put(subKey, new VCFFilterHeaderLine(subVal.getString("ID"), subVal.getString("Description")));
					}
				else if ("mFormatMetaData".equals(key))
					for (String subKey : ((org.bson.Document) val).keySet())
					{
						org.bson.Document subVal = (org.bson.Document) ((org.bson.Document) val).get(subKey);
						VCFHeaderLineCount countType = VCFHeaderLineCount.valueOf(subVal.getString("countType"));
						if (countType.equals(VCFHeaderLineCount.INTEGER))
							header.getmFormatMetaData().put(subKey, new VCFFormatHeaderLine(subVal.getString("name"), subVal.getInteger("count"), VCFHeaderLineType.valueOf(subVal.getString("type")), subVal.getString("description")));
						else
							header.getmFormatMetaData().put(subKey, new VCFFormatHeaderLine(subVal.getString("name"), countType, VCFHeaderLineType.valueOf(subVal.getString("type")), subVal.getString("description")));
					}
				else if ("mOtherMetaData".equals(key))
					for (String subKey : ((org.bson.Document) val).keySet())
					{
						org.bson.Document subVal = (org.bson.Document) ((org.bson.Document) val).get(subKey);
						header.getmOtherMetaData().put(subKey, new VCFHeaderLine(subVal.getString("mKey"), subVal.getString("mValue")));
					}
				else if ("mMetaData".equals(key))
					for (String subKey : ((org.bson.Document) val).keySet())
					{
						org.bson.Document subVal = (org.bson.Document) ((org.bson.Document) ((org.bson.Document) val).get(subKey)).get("genericFields");
						header.getmMetaData().put(subKey, new VCFSimpleHeaderLine(subKey, subVal.getString("ID"), subVal.getString("Description")));
					}
				else if (!"_id".equals(key))
					LOG.info("Unable to deal with org.bson.Document header attribute: " + key);
			}
		}
		return header;
	}
	
//	static public VCFHeaderLineCount getCount(BasicDBObject vcfCompoundHeaderLine) throws Exception
//	{
//		VCFHeaderLineCount countType = VCFHeaderLineCount.valueOf(vcfCompoundHeaderLine.getString("countType"));
//		switch (countType)
//		{
//			case INTEGER:
//				return vcfCompoundHeaderLine.getInt("count") + "";
//			case A:
//			case G:
//			case UNBOUNDED:
//				return ".";
//			default:
//				throw new Exception("Unable to find out VCF compound header-line count value");
//		}
//	}
}