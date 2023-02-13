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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpSession;

/**
 * Used for providing access to session attributes when running in a Thread asynchronous with the one that processed the original web request.
 * We don't want to keep a reference to the session itself because it may get invalidated before the thread actually tries to access the attributes.
 */
public class SessionAttributeAwareThread extends Thread {
	private Map<String, Object> sessionAttributes = new HashMap<>();
	
	public SessionAttributeAwareThread(HttpSession session) {
		storeSessionAttributes(session);
	}
	
	public Map<String, Object> getSessionAttributes() {
		return sessionAttributes;
	}
	
	public void storeSessionAttributes(HttpSession session) {
       Enumeration<String> attrNames = session.getAttributeNames();
       while (attrNames.hasMoreElements() ) {
            String name = attrNames.nextElement();
            sessionAttributes.put(name, session.getAttribute(name));
        }
	}
}