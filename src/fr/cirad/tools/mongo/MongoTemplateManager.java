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
package fr.cirad.tools.mongo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import fr.cirad.mgdb.model.mongo.maintypes.DatabaseInformation;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.AppConfig;
import fr.cirad.tools.Helper;

/**
 * The Class MongoTemplateManager.
 */
@Component
public class MongoTemplateManager implements ApplicationContextAware {

    /**
     * The Constant LOG.
     */
    static private final Logger LOG = Logger.getLogger(MongoTemplateManager.class);

    /**
     * The application context.
     */
    static private ApplicationContext applicationContext;

    /**
     * The template map.
     */
    static private Map<String, MongoTemplate> templateMap = new TreeMap<>();
    
    /**
     * The taxon map.
     */
    static private Map<String, String> taxonMap = new TreeMap<>();

    /**
     * The public databases.
     */
    static private Set<String> publicDatabases = new TreeSet<>();

    /**
     * The hidden databases.
     */
    static private List<String> hiddenDatabases = new ArrayList<>();

    /**
     * The mongo clients.
     */
    static private Map<String, MongoClient> mongoClients = new HashMap<>();
    
    /**
     * The datasource  (properties filename)
     */
    static private String resource = "datasources";
    
    /**
     * The datasource properties
     */  
    static private Properties dataSourceProperties = new Properties();
    
    /**
     * The expiry prefix.
     */
    static private String EXPIRY_PREFIX = "_ExpiresOn_";

    /**
     * The temp export prefix.
     */
    static public String TEMP_COLL_PREFIX = "tmpVar_";

    /**
     * The dot replacement string.
     */
    static final public String DOT_REPLACEMENT_STRING = "\\[dot\\]";

    /**
     * store ontology terms
     */
    static private Map<String, String> ontologyMap;

    /**
     * The app config.
     */
    @Autowired private AppConfig appConfig;
    
    private static final List<String> addressesConsideredLocal = Arrays.asList("127.0.0.1", "localhost");
    
	/** Map that associates modules to projects currently undergoing a write operation, thus making them unavailable for other write operations
	 *  A null value in the set indicates the whole module is locked (i.e., a dump is being generated or restored)
	 */
	private static HashMap<String /*module*/, Set<String> /*projects*/> currentlyImportedProjects = new HashMap<String, Set<String>>();

    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        initialize(ac);
        String serverCleanupCSV = appConfig.dbServerCleanup();
        List<String> authorizedCleanupServers = serverCleanupCSV == null ? null : Arrays.asList(serverCleanupCSV.split(","));

        // we do this cleanup here because it only happens when the webapp is being (re)started
        for (String sModule : templateMap.keySet()) {
        	List<ServerDescription> serverDescriptions = mongoClients.get(getModuleHost(sModule)).getClusterDescription().getServerDescriptions();
            MongoTemplate mongoTemplate = templateMap.get(sModule);
            if (authorizedCleanupServers == null || (serverDescriptions.size() == 1 && authorizedCleanupServers.contains(serverDescriptions.get(0).getAddress().toString()))) 
                new Thread() {
                    public void run() {
                        for (String collName : mongoTemplate.getCollectionNames()) {
                            if (collName.startsWith(TEMP_COLL_PREFIX)) {
                                mongoTemplate.dropCollection(collName);
                                LOG.debug("Dropped collection " + collName + " in module " + sModule);
                            }
                        }
                    }
                }.start();
        }
    }

    public static Map<String, MongoTemplate> getTemplateMap() {
        return templateMap;
    }

    /**
     * Initialize.
     *
     * @param ac the app-context
     * @throws BeansException the beans exception
     */
    static public void initialize(ApplicationContext ac) throws BeansException {
    	if (applicationContext != null)
    		return;	// already initialized
    	
        applicationContext = ac;
        while (applicationContext.getParent() != null) /* we want the root application-context */
            applicationContext = applicationContext.getParent();

        loadDataSources();
    }
    
    static public void clearExpiredDatabases() {
        try
        {
            Enumeration<Object> bundleKeys = dataSourceProperties.keys();
            while (bundleKeys.hasMoreElements()) {
				String key = (String) bundleKeys.nextElement();
				String[] datasourceInfo = dataSourceProperties.getProperty(key).split(",");
				
				if (datasourceInfo.length < 2) {
				    LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
				    continue;
				}
				
				if (datasourceInfo[1].contains(EXPIRY_PREFIX)) {
				    long expiryDate = Long.valueOf((datasourceInfo[1].substring(datasourceInfo[1].lastIndexOf(EXPIRY_PREFIX) + EXPIRY_PREFIX.length())));
				    if (System.currentTimeMillis() > expiryDate) {
				        if (removeDataSource(key, true))
				        	LOG.info("Removed expired datasource entry: " + key + " and temporary database: " + datasourceInfo[1]);
				    }
				}

            }
        }
        catch (MissingResourceException mre)
        {
            LOG.error("Unable to find file " + resource + ".properties, you may need to adjust your classpath", mre);
        }
    }

    /**
     * Load data sources.
     */
    static public void loadDataSources() {
        templateMap.clear();
        mongoClients.clear();
        publicDatabases.clear();
        hiddenDatabases.clear();
        try {
            mongoClients = applicationContext.getBeansOfType(MongoClient.class);
            
    	    InputStream input = MongoTemplateManager.class.getClassLoader().getResourceAsStream(resource + ".properties");
    	    dataSourceProperties.load(input);
    	    input.close();
            
            Enumeration<Object> bundleKeys = dataSourceProperties.keys();
            while (bundleKeys.hasMoreElements()) {
                String key = (String) bundleKeys.nextElement();
                String[] datasourceInfo = dataSourceProperties.getProperty(key).split(",");

                if (datasourceInfo.length < 2) {
                    LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
                    continue;
                }

                boolean fHidden = key.endsWith("*"), fPublic = key.startsWith("*");
                String cleanKey = key.replaceAll("\\*", "");
                if (cleanKey.length() == 0) {
                	LOG.warn("Skipping unnamed datasource");
                	continue;
                }

                if (templateMap.containsKey(cleanKey)) {
                    LOG.error("Datasource " + cleanKey + " already exists!");
                    continue;
                }

                try {
                	if (datasourceInfo.length > 2)
                		setTaxon(cleanKey, datasourceInfo[2]);
                    templateMap.put(cleanKey, createMongoTemplate(datasourceInfo[0], datasourceInfo[1]));
                    if (fPublic)
                        publicDatabases.add(cleanKey);
                    if (fHidden)
                        hiddenDatabases.add(cleanKey);
                    LOG.info("Datasource " + cleanKey + " loaded as " + (fPublic ? "public" : "private") + " and " + (fHidden ? "hidden" : "exposed"));
                }
                catch (UnknownHostException e) {
                    LOG.warn("Unable to create MongoTemplate for module " + cleanKey + " (no such host)");
                }
                catch (Exception e) {
                    LOG.warn("Unable to create MongoTemplate for module " + cleanKey, e);
                }
            }
        } catch (IOException ioe) {
            LOG.error("Unable to load " + resource + ".properties, you may need to adjust your classpath", ioe);
        }
    }

    /**
     * Creates the mongo template.
     *
     * @param sHost the host
     * @param sDbName the db name
     * @return the mongo template
     * @throws Exception the exception
     */
    public static MongoTemplate createMongoTemplate(String sHost, String sDbName) throws Exception {
        MongoClient client = mongoClients.get(sHost);
        if (client == null)
            throw new IOException("Unknown host: " + sHost);

        MongoTemplate mongoTemplate = new MongoTemplate(client, sDbName);
        ((MappingMongoConverter) mongoTemplate.getConverter()).setMapKeyDotReplacement(DOT_REPLACEMENT_STRING);
		mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));

		MgdbDao.ensurePositionIndexes(mongoTemplate, Arrays.asList(mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)), mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class))));	// make sure we have indexes defined as required in v2.4
        return mongoTemplate;
    }

    public enum ModuleAction {
    	CREATE, UPDATE_STATUS, DELETE;
    }
    
    /**
     * Saves or updates a data source.
     *
     * @param action the action to perform on the module
     * @param sModule the module, with a leading * if public and/or a trailing * if hidden
     * @param public flag telling whether or not the module shall be public, ignored for deletion
	 * @param hidden flag telling whether or not the module shall be hidden, ignored for deletion
     * @param sHost the host, only used for creation
     * @param ncbiTaxonIdNameAndSpecies id and scientific name of the ncbi taxon (colon-separated), optional, ignored for deletion
     * @param expiryDate the expiry date, only used for creation
     * @throws Exception the exception
     */
    synchronized static public boolean saveOrUpdateDataSource(ModuleAction action, String sModule, boolean fPublic, boolean fHidden, String sHost, String ncbiTaxonIdNameAndSpecies, Long expiryDate) throws Exception
    {	// as long as we keep all write operations in a single synchronized method, we should be safe
    	if (get(sModule) == null) {
    		if (!action.equals(ModuleAction.CREATE))
    			throw new Exception("Module " + sModule + " does not exist!");
    	}
    	else if (action.equals(ModuleAction.CREATE))
    		throw new Exception("Module " + sModule + " already exists!");
    	
    	FileOutputStream fos = null;
        File f = new ClassPathResource("/" + resource + ".properties").getFile();
    	FileReader fileReader = new FileReader(f);

        dataSourceProperties.load(fileReader);
        
    	try
    	{
    		if (action.equals(ModuleAction.DELETE))
    		{
    	        String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
                if (!dataSourceProperties.containsKey(sModuleKey))
                {
                	LOG.warn("Module could not be found in datasource.properties: " + sModule);
                	return false;
                }
                dataSourceProperties.remove(sModuleKey);
                fos = new FileOutputStream(f);
                dataSourceProperties.store(fos, null);
                return true;
    		}
	        else if (action.equals(ModuleAction.CREATE))
	        {
	            int nRetries = 0;
		        while (nRetries < 100)
		        {
		            String sIndexForModule = nRetries == 0 ? "" : ("_" + nRetries);
		            String sDbName = "mgdb2_" + sModule + sIndexForModule + (expiryDate == null ? "" : (EXPIRY_PREFIX + expiryDate));
		            MongoTemplate mongoTemplate = createMongoTemplate(sHost, sDbName);
		            if (mongoTemplate.getCollectionNames().size() > 0)
		                nRetries++;	// DB already exists, let's try with a different DB name
		            else
		            {
		                if (dataSourceProperties.containsKey(sModule) || dataSourceProperties.containsKey("*" + sModule) || dataSourceProperties.containsKey(sModule + "*") || dataSourceProperties.containsKey("*" + sModule + "*"))
		                {
		                	LOG.warn("Tried to create a module that already exists in datasource.properties: " + sModule);
		                	return false;
		                }
		                String sModuleKey = (fPublic ? "*" : "") + sModule + (fHidden ? "*" : "");
		                if (ncbiTaxonIdNameAndSpecies != null)
		                	setTaxon(sModule, ncbiTaxonIdNameAndSpecies);
		                dataSourceProperties.put(sModuleKey, sHost + "," + sDbName + "," + (ncbiTaxonIdNameAndSpecies == null ? "" : ncbiTaxonIdNameAndSpecies));
		                fos = new FileOutputStream(f);
		                dataSourceProperties.store(fos, null);

		                templateMap.put(sModule, mongoTemplate);
		                if (fPublic)
		                    publicDatabases.add(sModule);
		                if (fHidden)
		                    hiddenDatabases.add(sModule);
		                updateDatabaseLastModification(sModule);
		                return true;
		            }
		        }
		        throw new Exception("Unable to create a unique name for datasource " + sModule + " after " + nRetries + " retries");
	        }
	        else if (action.equals(ModuleAction.UPDATE_STATUS))
	        {
	        	String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
                if (!dataSourceProperties.containsKey(sModuleKey))
                {
                	LOG.warn("Tried to update a module that could not be found in datasource.properties: " + sModule);
                	return false;
                }
                String[] propValues = ((String) dataSourceProperties.get(sModuleKey)).split(",");
                dataSourceProperties.remove(sModuleKey);
                if (ncbiTaxonIdNameAndSpecies == null && getTaxonId(sModule) != null)
                {
                	String taxonName = getTaxonName(sModule), species = getSpecies(sModule);
                	ncbiTaxonIdNameAndSpecies = getTaxonId(sModule) + ":" + (species != null && species.equals(taxonName) ? "" : taxonName) + ":" + (species != null ? species : "");
                }
                dataSourceProperties.put((fPublic ? "*" : "") + sModule + (fHidden ? "*" : ""), propValues[0] + "," + propValues[1] + "," + ncbiTaxonIdNameAndSpecies);
                fos = new FileOutputStream(f);
                dataSourceProperties.store(fos, null);
                
                if (fPublic)
                    publicDatabases.add(sModule);
                else
                	publicDatabases.remove(sModule);
                if (fHidden)
                    hiddenDatabases.add(sModule);
                else
                	hiddenDatabases.remove(sModule);
	        	return true;
	        }
	        else
	        	throw new Exception("Unknown ModuleAction: " + action);
        }
    	catch (IOException ex)
    	{
            LOG.warn("Failed to update datasource.properties for action " + action + " on " + sModule, ex);
            return false;
        }
    	finally
    	{
            try 
            {
           		fileReader.close();
            	if (fos != null)
            		fos.close();
            } 
            catch (IOException ex)
            {
                LOG.debug("Failed to close FileReader", ex);
            }
        }
    }

    /**
     * Removes the data source.
     *
     * @param sModule the module
     * @param fAlsoDropDatabase whether or not to also drop database
     */
    static public boolean removeDataSource(String sModule, boolean fAlsoDropDatabase)
    {
        try
        {
            String key = sModule.replaceAll("\\*", "");
        	saveOrUpdateDataSource(ModuleAction.DELETE, key, false, false, null, null, null);	// only this unique synchronized method may write to file safely

            if (fAlsoDropDatabase)
                templateMap.get(key).getDb().drop();
            templateMap.remove(key);
            publicDatabases.remove(key);
            hiddenDatabases.remove(key);
            return true;
        }
        catch (Exception ex)
        {
            LOG.warn("Failed to remove " + sModule + " from datasource.properties", ex);
            return false;
        }
    }

    /**
     * fill the ontology map
     *
     * @param newOntologyMap
     */
    public static void setOntologyMap(Map<String, String> newOntologyMap) {
        ontologyMap = newOntologyMap;
    }

    /**
     * getter for ontology map
     *
     * @return
     */
    public static Map<String, String> getOntologyMap() {
        return ontologyMap;
    }

    /**
     * Gets the host names.
     *
     * @return the host names
     */
    static public Set<String> getHostNames() {
        return mongoClients.keySet();
    }

    /**
     * Gets the.
     *
     * @param module the module
     * @return the mongo template
     */
    static public MongoTemplate get(String module) {
        return templateMap.get(module);
    }

    /**
     * Gets the public database names.
     *
     * @return the public database names
     */
    static public Collection<String> getPublicDatabases() {
        return publicDatabases;
    }

    static public void dropAllTempColls(Collection<String> tokens) {
    	List<String> tempCollNames = tokens.stream().filter(token -> token != null && !tokens.isEmpty()).map(token -> MongoTemplateManager.TEMP_COLL_PREFIX + Helper.convertToMD5(token)).collect(Collectors.toList());
        if (tokens.isEmpty())
            return;
    	
        new Thread() {
            public void run() {
                for (String module : MongoTemplateManager.getTemplateMap().keySet()) {
                    for (String tempCollName : tempCollNames) { // drop all temp collections associated to this token in this module
                        templateMap.get(module).getCollection(tempCollName).drop();
                        LOG.debug("Dropped " + module + "." + tempCollName + " from dropAllTempColls");
                    }
                }
            }
        }.start();
    }

    /**
     * Gets the available modules.
     *
     * @return the available modules
     */
    static public Collection<String> getAvailableModules() {
        return templateMap.keySet();
    }

    /**
     * Checks if is module public.
     *
     * @param sModule the module
     * @return true, if is module public
     */
    static public boolean isModulePublic(String sModule) {
        return publicDatabases.contains(sModule);
    }

    /**
     * Checks if is module hidden.
     *
     * @param sModule the module
     * @return true, if is module hidden
     */
    static public boolean isModuleHidden(String sModule) {
        return hiddenDatabases.contains(sModule);
    }

//	public void saveRunsIntoProjectRecords()
//	{
//		for (String module : getAvailableModules())
//		{
//			MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
//			for (GenotypingProject proj : mongoTemplate.findAll(GenotypingProject.class))
//				if (proj.getRuns().size() == 0)
//				{
//					boolean fRunAdded = false;
//					for (String run : (List<String>) mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(VariantData.class)).distinct(VariantData.FIELDNAME_PROJECT_DATA + "." + proj.getId() + "." + Run.RUNNAME))
//						if (!proj.getRuns().contains(run))
//						{
//							proj.getRuns().add(run);
//							LOG.info("run " + run + " added to project " + proj.getName() + " in module " + module);
//							fRunAdded = true;
//						}
//					if (fRunAdded)
//						mongoTemplate.save(proj);
//				}
//		}
//	}
    /**
     * Gets the mongo collection name.
     *
     * @param clazz the class
     * @return the mongo collection name
     */
    public static String getMongoCollectionName(Class clazz) {
    	org.springframework.data.mongodb.core.mapping.Document document = (org.springframework.data.mongodb.core.mapping.Document) clazz.getAnnotation(org.springframework.data.mongodb.core.mapping.Document.class);
        if (document != null) {
            return document.collection();
        }
        return clazz.getSimpleName();
    }

	public static void setTaxon(String database, String taxon) {
		taxonMap.put(database, taxon);
	}
	
	public static Integer getTaxonId(String database) {
		String taxon = taxonMap.get(database);
		if (taxon == null)
			return null;
		String[] splitTaxonDetails = taxon.split(":");
		if (splitTaxonDetails.length < 1)
			return null;
		
		try {
			return Integer.parseInt(splitTaxonDetails[0]);
		}
		catch (NumberFormatException ignored) {
			return null;
		}
	}
	
	public static String getTaxonName(String database) {
		String taxon = taxonMap.get(database);
		if (taxon == null)
			return null;
		String[] splitTaxonDetails = taxon.split(":");
		if (splitTaxonDetails.length < 2)
			return null;

		String taxonName = splitTaxonDetails[1].isEmpty() && splitTaxonDetails.length > 2 ? splitTaxonDetails[2] : splitTaxonDetails[1];
		return "".equals(taxonName) ? null : taxonName;
	}

	public static String getSpecies(String database) {
		String taxon = taxonMap.get(database);
		if (taxon == null)
			return null;
		String[] splitTaxonDetails = taxon.split(":");
		if (splitTaxonDetails.length < 3)
			return null;
		
		return splitTaxonDetails.length > 2 ? splitTaxonDetails[2] : null;
	}

    public static String getModuleHost(String sModule) {
        Enumeration<Object> bundleKeys = dataSourceProperties.keys();
        while (bundleKeys.hasMoreElements()) {
            String key = (String) bundleKeys.nextElement();
            
            if (sModule.equals(key.replaceAll("\\*", ""))) {
            	String[] datasourceInfo = dataSourceProperties.getProperty(key).split(",");
            	return datasourceInfo[0];
            }
        }
        return null;
    }
    
    public static boolean isModuleOnLocalHost(String sModule) {
    	String sHost = getModuleHost(sModule);
    	MongoClient client = mongoClients.get(sHost);
    	ClusterDescription clusterDesc = client.getClusterDescription();
    	List<ServerDescription> descs = clusterDesc.getServerDescriptions();
    	for (ServerDescription desc : descs)
    		if (!addressesConsideredLocal.contains(desc.getAddress().getHost()))
    			return false;
    	return true;
    }
    
    public static List<String> getServerHosts(String sHost) {
    	MongoClient client = mongoClients.get(sHost);
    	ClusterDescription cluster = client.getClusterDescription();
    	List<ServerDescription> servers = cluster.getServerDescriptions();
    	List<String> hosts = new ArrayList<String>();
    	for (ServerDescription desc : servers) {
    		ServerAddress address = desc.getAddress();
    		hosts.add(address.getHost() + ":" + address.getPort());
    	}
    	return hosts;
    }
    
    public static String getDatabaseName(String sModule) {
    	String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
    	String dataSource = dataSourceProperties.getProperty(sModuleKey);
    	return dataSource.split(",")[1];
    }
    
	public static boolean isModuleAvailableForWriting(String sModule) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			return projects.size() == 0;
		} else {
			return true;
		}
	}

	public static void lockProjectForWriting(String sModule, String sProject) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			projects.add(sProject);
		} else {
			projects = new HashSet<String>();
			projects.add(sProject);
			currentlyImportedProjects.put(sModule, projects);
		}
	}

	public static void unlockProjectForWriting(String sModule, String sProject) {
		currentlyImportedProjects.get(sModule).remove(sProject);
	}

	public static void lockModuleForWriting(String sModule) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			projects.add(null);
		} else {
			projects = new HashSet<String>();
			projects.add(null);
			currentlyImportedProjects.put(sModule, projects);
		}
	}

	public static void unlockModuleForWriting(String sModule) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			projects.clear();
		}
	}
	
    public static void updateDatabaseLastModification(String sModule) {
    	MongoTemplateManager.updateDatabaseLastModification(sModule, new Date(), false);
    }
    
    public static void updateDatabaseLastModification(String sModule, Date lastModification, boolean restored) {
    	MongoTemplate template = MongoTemplateManager.get(sModule);
    	
    	Update update = new Update();
    	update.set(DatabaseInformation.FIELDNAME_LAST_MODIFICATION, lastModification);
    	update.set(DatabaseInformation.FIELDNAME_RESTORE_DATE, restored ? new Date() : null);
    	template.upsert(new Query(), update, "dbInfo");
    }
    
    public static DatabaseInformation getDatabaseInformation(String sModule) {
    	MongoTemplate template = MongoTemplateManager.get(sModule);
    	return template.findOne(new Query(), DatabaseInformation.class, "dbInfo");
    }
}
