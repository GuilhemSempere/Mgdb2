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
package fr.cirad.mgdb.exporting;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;

import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;

/**
 * The class AbstractExportWritingThread.
 */
public abstract class AbstractExportWritingThread extends Thread
{
	/** The Constant LOG. */
	static final Logger LOG = Logger.getLogger(AbstractExportWritingThread.class);
	
	protected Collection<Collection<VariantRunData>> markerRunsToWrite;
	
	public CompletableFuture<Void> writeRuns(Collection<Collection<VariantRunData>> markerRunsToWrite) {
		this.markerRunsToWrite = markerRunsToWrite;
		return CompletableFuture.runAsync(this);
	}
	
	public abstract void run();
}