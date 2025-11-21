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
package fr.cirad.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 * The Class AppConfig.
 */
@Configuration
public class AppConfig {
	
	static private final Logger LOG = Logger.getLogger(AppConfig.class);
	
	static private AbstractConfigManager configManager;
	@Autowired void setConfigManager(AbstractConfigManager cm) {
		configManager = cm;
	}

	public final String CONFIG_FILE = "config";
	
    /**
     * The resource control.
     */
    private static final Control resourceControl = new ResourceBundle.Control() {
        @Override
        public boolean needsReload(String baseName, java.util.Locale locale, String format, ClassLoader loader, ResourceBundle bundle, long loadTime) {
            return true;
        }

        @Override
        public long getTimeToLive(String baseName, java.util.Locale locale) {
            return 0;
        }
    };
    
    private ResourceBundle props = new ResourceBundle() {
    	boolean initialized = false;
    	Map<String, String> envProperties;
    	ResourceBundle innerRB;
    	
    	private void init() throws SecurityException {
        	envProperties = System.getenv().entrySet().stream().filter(e -> e.getKey().startsWith(configManager.getPropertyOverridingPrefix())).collect(Collectors.toMap(e -> ((String) e.getKey()).substring(configManager.getPropertyOverridingPrefix().length()), Map.Entry::getValue));
        	for (String forbiddenKey : configManager.getNonOverridablePropertyNames())
	        	if (envProperties.containsKey(forbiddenKey))
	        		throw new SecurityException("Overriding casServerURL via env variable is not allowed");

        	innerRB = ResourceBundle.getBundle(CONFIG_FILE, resourceControl);
    	}

		@Override
		protected Object handleGetObject(String key) {
    		if (!initialized)
    			init();

			Object envValue = envProperties.get(key);
			return envValue != null ? envValue : innerRB.getObject(key);
		}

		@Override
		public Enumeration<String> getKeys() {
    		if (!initialized)
    			init();

			Set<String> propKeys = new HashSet<>(envProperties.keySet());
			Enumeration<String> rbKeys = innerRB.getKeys();
			while (rbKeys.hasMoreElements())
				propKeys.add(rbKeys.nextElement());
			return Collections.enumeration(propKeys);
		}
    };

    public String get(String sPropertyName, String defaultValue) {
        String val = get(sPropertyName);
        return val == null ? defaultValue : val;
    }
    
    public String get(String sPropertyName) {
        return props.containsKey(sPropertyName) ? props.getString(sPropertyName) : null;
    }
    
    public Long getLong(String sPropertyName, Long defaultValue) {
    	Long val = getLong(sPropertyName);
    	return val == null ? defaultValue : val;
    }
    
    public Long getLong(String sPropertyName) {
    	String val = get(sPropertyName);
    	try {
    		return Double.valueOf(val).longValue();
    	}
    	catch (Exception e) {
    		return null;
    	}
    }

    public Set<String> keySet() {
        return props.keySet();
    }

    public LinkedHashMap<String, String> getMandatoryMetadataFields(String sModule, boolean fForSamples) {
        String configInfo = null;
        String mdType = fForSamples ? "Sample" : "Individual";
        if (sModule != null && !sModule.isEmpty())
            configInfo = get("mandatory" + mdType + "Metadata-" + sModule);
        if (configInfo == null)
            configInfo = get("mandatory" + mdType + "Metadata");

        LinkedHashMap<String, String> result = new LinkedHashMap<>() {{ put(mdType.toLowerCase(), "Must match the values provided with the genotyping data");}} ;
        if (configInfo != null) {
            String[] pairs = configInfo.split(";(?![^\\[\\]]*\\])");
            for (String pair : pairs) {
                pair = pair.trim();
                int openBracket = pair.indexOf('[');
                if (openBracket == -1)
                    result.put(pair, "");
                else {
                    String fieldName = pair.substring(0, openBracket).trim();
                    int closeBracket = pair.lastIndexOf(']');
                    if (closeBracket == -1)
                        result.put(fieldName, pair.substring(openBracket + 1).trim());
                    else {
                        String description = pair.substring(openBracket + 1, closeBracket).trim();
                        result.put(fieldName, description.isEmpty() ? null : description);
                    }
                }
            }
        }
        return result;
    }

    synchronized public String getInstanceUUID() throws IOException {
        String instanceUUID = get("instanceUUID");
        if (instanceUUID == null) { // generate it
        	String generatedInstanceUUID = UUID.randomUUID().toString();
        	saveProperties(new HashMap<>() {{ put("instanceUUID", generatedInstanceUUID); }});
        	instanceUUID = generatedInstanceUUID;
        }
        return instanceUUID;
    }
    
    synchronized public void saveProperties(Map<String, String> propsToSet) throws IOException {
    	FileOutputStream fos = null;
        File f = new ClassPathResource("/" + CONFIG_FILE + ".properties").getFile();
    	FileReader fileReader = new FileReader(f);
        Properties properties = new Properties();
        properties.load(fileReader);
        for (String key : propsToSet.keySet()) {
        	String value = propsToSet.get(key);
	        if (value == null) {
	        	properties.remove(key);
	        	LOG.info("Removing " + key + " config-property");
	        }
	        else {
	        	properties.put(key, value);
	        	LOG.info("Saving " + key + " config-property as " + value);
	        }
        }
        fos = new FileOutputStream(f);
        properties.store(fos, null);
        props = new ResourceBundle() {
	        @Override
	        protected Object handleGetObject(String key) {
	            return properties.getProperty(key);
	        }
	
	        @Override
	        public Enumeration<String> getKeys() {
	            return Collections.enumeration(properties.stringPropertyNames());
	        }
	    };
	}
    
    public Map<String, String> getPrefixed(String sPrefix) {
        Map<String, String> result = new HashMap<>();
        Enumeration<String> keys = props.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            if (key.startsWith(sPrefix))
                    result.put(key, get(key));
        }
        return result;
    }

    public int getAlleleSearchMaxTotalPageSize() {
        try {
            String value = get("alleleSearchMaxTotalPageSize");
            if (value != null) {
                return Integer.parseInt(value);
            }
        } catch (NumberFormatException e) {
            LOG.info("Invalid value for maxAlleleSearchTotalCount in config, using default: 10000", e);
        }
        return 10000; // default value
    }
}