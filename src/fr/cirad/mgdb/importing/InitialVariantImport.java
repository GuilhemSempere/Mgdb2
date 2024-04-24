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
package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;

import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class VariantSynonymImport.
 */
public class InitialVariantImport {
    
    /** The Constant LOG. */
    private static final Logger LOG = Logger.getLogger(InitialVariantImport.class);

    static public final String VARIANT_LIST_COLNAME_ID = "id";
    static public final String VARIANT_LIST_COLNAME_TYPE = "type";
    static public final String VARIANT_LIST_COLNAME_CHIP = "chip";
    static public final String VARIANT_LIST_COLNAME_ALLELES = "all";

    /** The Constant twoDecimalNF. */
    static private final NumberFormat twoDecimalNF = NumberFormat.getInstance();
    
    static public final List<String> synonymColNames = Arrays.asList(VariantData.FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA, VariantData.FIELDNAME_SYNONYM_TYPE_ID_NCBI, VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL);
    
    static private final String ASSEMBLY_POSITION_PREFIX = "pos-";
    
    private ProgressIndicator progress = null;

    static
    {
        twoDecimalNF.setMaximumFractionDigits(2);
    }

    public InitialVariantImport(ProgressIndicator progress) {
		this.progress = progress;
	}

	/**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception
    {
        /* Insert chip list from external file */
//      HashMap<String, HashSet<String>> illuminaIdToChipMap = new HashMap<String, HashSet<String>>();
//      Scanner sc = new Scanner(new File("/media/sempere/Seagate Expansion Drive/D/data/intertryp/SNPchimp_exhaustive_with_rs.tsv"));
//      sc.nextLine();
//      while (sc.hasNextLine())
//      {
//          String[] splittedLine = sc.nextLine().split("\t");
//          HashSet<String> chipsForId = illuminaIdToChipMap.get(splittedLine[4]);
//          if (chipsForId == null)
//          {
//              chipsForId = new HashSet<String>();
//              illuminaIdToChipMap.put(splittedLine[4], chipsForId);
//          }
//          chipsForId.add(splittedLine[0]);
//      }
//      
//      sc.close();
//      sc = new Scanner(new File("/media/sempere/Seagate Expansion Drive/D/data/intertryp/bos_ExhaustiveSnpList_StandardFormat.tsv"));
//      FileWriter fw = new FileWriter("/media/sempere/Seagate Expansion Drive/D/data/intertryp/bos_ExhaustiveSnpList_StandardFormat_wChips.tsv");
//      fw.write(sc.nextLine() + "\tchip\n");
//      while (sc.hasNextLine())
//      {
//          String sLine = sc.nextLine();
//          String[] splittedLine = sLine.split("\t");
//          HashSet<String> chipsForId = illuminaIdToChipMap.get(splittedLine[2].split(";")[0]);
//          fw.write(sLine + "\t" + StringUtils.join(chipsForId, ";") + "\n");
//      }
//      sc.close();
//      fw.close();
     
        if (args.length < 2)
            throw new Exception("You must pass 2 parameters as arguments: DATASOURCE name, exhaustive variant list TSV file.");

        new InitialVariantImport(null).insertVariantsAndSynonyms(args[0], args[1]);
    }
    
    public void insertVariantsAndSynonyms(String sModule, String sImportFilePath) throws Exception
    {
        File chipInfoFile = new File(sImportFilePath);
        if (!chipInfoFile.exists() || chipInfoFile.isDirectory())
            throw new Exception("Data file does not exist: " + chipInfoFile.getAbsolutePath());
        
        GenericXmlApplicationContext ctx = null;
        try
        {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null)
            {   // we are probably being invoked offline
                ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
    
                MongoTemplateManager.initialize(ctx);
                mongoTemplate = MongoTemplateManager.get(sModule);
                if (mongoTemplate == null)
                    throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
            }

//          if (Helper.estimDocCount(mongoTemplate, VariantData.class) > 0)
//          	throw new Exception("There are already some variants in this database!");
            
            MongoCollection<Document> varColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)), vrdColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class));
            boolean fUpdateExistingList = Helper.estimDocCount(sModule, VariantData.class) > 0;
            String backupCollName = !fUpdateExistingList ? null : varColl.getNamespace().getCollectionName() + "_backup_" + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
            if (fUpdateExistingList)
            	varColl.renameCollection(new MongoNamespace(varColl.getNamespace().getDatabaseName(), backupCollName));
            
            long before = System.currentTimeMillis();

            BufferedReader in = new BufferedReader(new FileReader(chipInfoFile));
            try
            {
                String sLine = in.readLine();   // read header
                if (sLine != null)
                    sLine = sLine.trim();
                List<String> header = splitByComaSpaceOrTab(sLine);
                sLine = in.readLine();
//                if (sLine != null)
//                    sLine = sLine.trim();
                
                List<String> fieldsExceptSynonyms = new ArrayList<>();
                List<Assembly> assemblies = new ArrayList<>();
                for (String colName : header) {
                    if (!synonymColNames.contains(colName))
                        fieldsExceptSynonyms.add(colName);
                    if (colName.startsWith(ASSEMBLY_POSITION_PREFIX)) {
                        String assemblyName = colName.substring(ASSEMBLY_POSITION_PREFIX.length());
                        Assembly assembly = mongoTemplate.findOne(new Query(Criteria.where(Assembly.FIELDNAME_NAME).is(assemblyName)), Assembly.class);
                        if (assembly == null) {
                            assembly = new Assembly(AutoIncrementCounter.getNextSequence(mongoTemplate, Assembly.class));
                            assembly.setName(assemblyName);
                            mongoTemplate.save(assembly);
                            LOG.info("Assembly \"" + assemblyName + "\" created for module " + sModule);
                        }
                        assemblies.add(assembly);
                    }
                }
                if (assemblies.isEmpty()) {
                    if (!header.contains("pos"))
                        throw new Exception("No position column could be found");
                    assemblies.add(null);   // means default, unnamed assembly
                }

                int idColIndex = header.indexOf(VARIANT_LIST_COLNAME_ID), typeColIndex = header.indexOf(VARIANT_LIST_COLNAME_TYPE), chipColIndex = header.indexOf(VARIANT_LIST_COLNAME_CHIP), alleleColIndex = header.indexOf(VARIANT_LIST_COLNAME_ALLELES);
                AtomicLong count = new AtomicLong();
                int nCurrentVariant = 0, nNumberOfVariantsToSaveAtOnce = fUpdateExistingList ? 10000 : 50000;
                AtomicReference<ArrayList<VariantData>> unsavedVariants = new AtomicReference<>(new ArrayList<>(nNumberOfVariantsToSaveAtOnce));
                AtomicReference<BulkOperations> vrdBulkOps = !fUpdateExistingList ? null : new AtomicReference<>(mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, VariantRunData.class));
                ExecutorService executor = Executors.newFixedThreadPool(8);
                
                // in order to avoid Spring adding _class fields to each persisted ReferencePosition, we need to convert them to Documents ourselves
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);
                objectMapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
                    @Override
                    public String findImplicitPropertyName(AnnotatedMember member) {
                        String fieldName = super.findImplicitPropertyName(member);
                        if (fieldName == null) {
                            Field fieldAnnotation = member.getAnnotation(Field.class);
                            if (fieldAnnotation != null)
                            	fieldName = fieldAnnotation.value();
                            else {
                            	BsonProperty bsonPropertyAnnotation = member.getAnnotation(BsonProperty.class);
                            	if (bsonPropertyAnnotation != null)
                            		fieldName = bsonPropertyAnnotation.value();
                            }
                        }
                        return fieldName;
                    }
                });

                final MongoTemplate finalMongoTemplate = mongoTemplate;
                do
                {
                    if (sLine.length() > 0)
                    {
                        List<String> cells = Helper.split(sLine, "\t");
                        if (cells.size() < 7)
                        	LOG.warn("Skipping incomplete line: " + sLine);
                        VariantData variant = new VariantData(cells.get(idColIndex));
                        variant.setType(cells.get(typeColIndex));
                        if (alleleColIndex != -1) {
	                        String alleles = cells.get(alleleColIndex);
	                        if (alleles != null) {
	                        	alleles = alleles.trim();
	                        	if (!alleles.isEmpty())
	                        		variant.setKnownAlleles(Helper.split(alleles, ";"));
	                        }
                        }
                        for (Assembly assembly : assemblies) {
                        	int cellIndex = header.indexOf(assembly == null ? "pos" : (ASSEMBLY_POSITION_PREFIX + assembly.getName()));
                            String[] seqAndPos = /*cellIndex >= cells.size() ? new String[0] : */cells.get(cellIndex).split(":");
                            if (seqAndPos.length == 2 && !seqAndPos[0].equals("0"))
                                variant.setReferencePosition(assembly == null ? 0 : assembly.getId(), new ReferencePosition(seqAndPos[0], Long.parseLong(seqAndPos[1])));
                        }
                        
                        if (!variant.getId().toString().startsWith("*"))    // otherwise it's a deprecated variant that we don't want to appear
                        {
                            String chipList = cells.get(chipColIndex);
                            if (chipList.length() > 0)
                            {
                                TreeSet<String> analysisMethods = new TreeSet<String>();
                                    for (String chip : chipList.split(";"))
                                        analysisMethods.add(chip);
                                variant.setAnalysisMethods(analysisMethods);
                            }
                        }

                        for (int i=0; i<header.size(); i++)
                        {
                            if (fieldsExceptSynonyms.contains(header.get(i)))
                                continue;

                            String syns = cells.get(i);
                            if (syns.length() > 0)
                            {
                                TreeSet<String> synSet = new TreeSet<>();
                                for (String syn : syns.split(";"))
                                    if (!syn.equals("."))
                                        synSet.add(syn);
                                variant.putSynonyms(header.get(i), synSet);
                            }
                        }
                        
                        unsavedVariants.get().add(variant);
                        if (fUpdateExistingList && variant.getPositions() != null) {
                            Update update = new Update();
                            update.set(VariantRunData.FIELDNAME_POSITIONS, Document.parse(objectMapper.writeValueAsString(variant.getPositions())));
                            vrdBulkOps.get().updateMulti(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID).is(variant.getId())), update);
                        }
                                                
                        if ((nCurrentVariant + 1) % nNumberOfVariantsToSaveAtOnce == 0) {
                        	ArrayList<VariantData> variantsToSave = unsavedVariants.get();
                        	BulkOperations chunkBulkOps = !fUpdateExistingList ? null : vrdBulkOps.get();
                        	if (fUpdateExistingList)
	                        	vrdBulkOps.set(finalMongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, VariantRunData.class));

                        	unsavedVariants.set(new ArrayList<>(nNumberOfVariantsToSaveAtOnce));
                        	executor.execute(new Thread() {
                        		public void run() {
	                                if (fUpdateExistingList) {
	                                	BulkWriteResult runBwr = chunkBulkOps.execute();
	                                	int nVrdModifCount = runBwr.getModifiedCount();
	                                	if (nVrdModifCount > 0)
	                                		LOG.debug(nVrdModifCount + " variantRunData records' position fields were updated");
	                                }
	                                count.addAndGet(finalMongoTemplate.insert(variantsToSave, VariantData.class).size());
                                    String info = count + " variants imported";
                                    if (progress != null)
                                    	progress.setCurrentStepProgress(count.get());
                                    LOG.debug(info);
                        		}
                        	});
                        }
                        
                        nCurrentVariant++;
                    }
                    sLine = in.readLine();
//                    if (sLine != null)
//                        sLine = sLine.trim();
                }
                while (sLine != null);
                
                if (unsavedVariants.get().size() > 0) {
                	ArrayList<VariantData> variantsToSave = unsavedVariants.get();
                	BulkOperations chunkBulkOps = !fUpdateExistingList ? null : vrdBulkOps.get();
                	executor.execute(new Thread() {
                		public void run() {
                            if (fUpdateExistingList) {
                            	BulkWriteResult runBwr = chunkBulkOps.execute();
                            	int nVrdModifCount = runBwr.getModifiedCount();
                            	if (nVrdModifCount > 0)
                            		LOG.debug(nVrdModifCount + " variantRunData records' position fields were updated");
                            }
                            count.addAndGet(finalMongoTemplate.insert(variantsToSave, VariantData.class).size());
                            String info = count + " variants imported";
                            if (progress != null)
                            	progress.setCurrentStepProgress(count.get());
                            LOG.debug(info);
                		}
                	});
                }

                executor.shutdown();
                executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

                if (fUpdateExistingList) {	// we need to make sure known-alleles info is not lost, and to update projects' sequence lists
                	executor = Executors.newFixedThreadPool(8);
                    if (progress != null) {
	                	progress.addStep("Making sure known alleles are conserved");
	                	progress.moveToNextStep();
                    }
                	
                	executor.execute(new Thread() {
                		public void run() {
                        	Query q = new Query(Criteria.where(VariantData.FIELDNAME_KNOWN_ALLELES).exists(true));
                        	q.fields().include(VariantData.FIELDNAME_KNOWN_ALLELES);
                        	 finalMongoTemplate.getCollection(backupCollName).find(q.getQueryObject()).projection(q.getFieldsObject()).forEach(withCounter((i, v) -> {
                        		Update update = new Update().set(VariantData.FIELDNAME_KNOWN_ALLELES, v.get(VariantData.FIELDNAME_KNOWN_ALLELES));
                        		finalMongoTemplate.updateFirst(new Query(Criteria.where("_id").is(v.get("_id"))), update, VariantData.class);
                        		if (i > 0 && i % 10000 == 0) {
        	                        String info = i + " lines processed in conserving known alleles";
        	                        if (progress != null)
        	                        	progress.setCurrentStepProgress(i);
                                    LOG.debug(info);
                        		}
                        	}));	
                		}
                	});

                	List<GenotypingProject> projects = mongoTemplate.find(new Query(), GenotypingProject.class);
                	for (GenotypingProject project : projects) {
                        if (progress != null) {
    	                	progress.addStep("Updating project " + project.getName() + "'s sequence lists");
    	                	progress.moveToNextStep();
                        }
	                	for (Assembly assembly : assemblies)
	                    	executor.execute(new Thread() {
	                    		public void run() {
		                		Collection<String> sequencesUsed = finalMongoTemplate.findDistinct(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId())), VariantData.FIELDNAME_POSITIONS + "." + assembly.getId() + "." + ReferencePosition.FIELDNAME_SEQUENCE, VariantRunData.class, String.class);
		                		project.getContigs().put(assembly.getId(), new TreeSet<>(sequencesUsed));
                                LOG.debug("Updated used sequence list for project '" + project.getName() + "' and assembly '" + assembly.getName() + "' -> " + sequencesUsed);
		                	}
	                    });
                	}
                    executor.shutdown();
                    executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
                    
                    for (GenotypingProject project : projects)
                		finalMongoTemplate.save(project);
                }

                LOG.info("InitialVariantImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
            }
            finally
            {
                in.close();         
            }
        }
        finally
        {
            if (ctx != null)
                ctx.close();
        }
    }

    /**
     * Split by coma space or tab.
     *
     * @param s the s
     * @return the list
     */
    private static List<String> splitByComaSpaceOrTab(String s)
    {
        return Helper.split(s, s.contains(",") ? "," : (s.contains(" ") ? " " : "\t"));
    }

	public void updateExistingVariantList(String[] args) {
		// TODO Auto-generated method stub
		
	}
	
	public static <T> Consumer<T> withCounter(BiConsumer<Integer, T> consumer) {
	    AtomicInteger counter = new AtomicInteger(0);
	    return item -> consumer.accept(counter.getAndIncrement(), item);
	}
}
