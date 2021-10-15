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
package fr.cirad.mgdb.exporting.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoCursor;

import fr.cirad.mgdb.exporting.markeroriented.AbstractMarkerOrientedExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ProgressIndicator;

public class AsyncExportToolV2 {
	
	private static final Logger LOG = Logger.getLogger(AbstractMarkerOrientedExportHandler.class);
	
	// This is a compromise: 3 seems to be enough for hand-coded variant oriented formats, VCF seems happy with 4, and individual oriented formats would probably benefit from 10 or more... make it dynamic?
    public static final int WRITING_QUEUE_CAPACITY = 4;

	private MongoCursor<Document> markerCursor;
	private MongoTemplate mongoTemplate;
	private int nQueryChunkSize;
	private List<GenotypingSample> samples;
	private ProgressIndicator progress;
	private AbstractDataOutputHandler<Integer, LinkedHashMap/*<VariantData, Collection<VariantRunData>>*/> dataOutputHandler;

	private int queryChunkIndex = 0, lastWrittenChunkIndex = -1;
	private ConcurrentSkipListMap<Integer, LinkedHashMap/*<VariantData, Collection<VariantRunData>>*/> dataWritingQueue;
	private boolean fHasBeenLaunched = false;
	
	private int nNumberOfChunks;

	public AsyncExportToolV2(MongoCursor<Document> markerCursor, long nTotalMarkerCount, int nQueryChunkSize, MongoTemplate mongoTemplate, List<GenotypingSample> samples, AbstractDataOutputHandler<Integer, LinkedHashMap/*<VariantData, Collection<VariantRunData>>*/> dataOutputHandler, ProgressIndicator progress) throws Exception
	{
		if (!markerCursor.hasNext())
			throw new Exception("markerCursor contains no data!");

		this.mongoTemplate = mongoTemplate;
		this.markerCursor = markerCursor;
		this.nQueryChunkSize = nQueryChunkSize;
		this.samples = samples;
		this.progress = progress;
		this.nNumberOfChunks = (int) Math.ceil((float) nTotalMarkerCount / nQueryChunkSize);
		this.dataWritingQueue = new ConcurrentSkipListMap<>();
		this.dataOutputHandler = dataOutputHandler;
	}

	public void launch() throws Exception
	{
		if (fHasBeenLaunched)
			throw new Exception("This AsyncExportTool has already been launched!");

		new Thread()
		{
			public void run()
			{
				launchWritingProcess();
			}
		}.start();

		for (int i=0; i<WRITING_QUEUE_CAPACITY; i++)
			tryAndGetNextChunks();	// writing will automatically start when first chunk is added to the queue
	}
	
	synchronized protected void tryAndGetNextChunks()
	{
		fHasBeenLaunched = true;

		if (!markerCursor.hasNext())
			return;	// finished
		
		int nLoadedMarkerCountInLoop = 0;
		boolean fStartingNewChunk = true;
		List<Object> currentMarkers = new ArrayList<>();
		while (markerCursor.hasNext() && (fStartingNewChunk || nLoadedMarkerCountInLoop%nQueryChunkSize != 0))
		{
			Document exportVariant = markerCursor.next();
			currentMarkers.add((Comparable) exportVariant.get("_id"));
			nLoadedMarkerCountInLoop++;
			fStartingNewChunk = false;
		}

		final int nFinalChunkIndex = queryChunkIndex++;
		new Thread()
		{
			public void run()
			{
				try
				{
					if (mongoTemplate.getDb().getName().startsWith("mgdb2_"))
						dataWritingQueue.put(nFinalChunkIndex, MgdbDao.getSampleGenotypesV2(mongoTemplate, samples, currentMarkers, true, null));
					else
						dataWritingQueue.put(nFinalChunkIndex, MgdbDao.getSampleGenotypes(mongoTemplate, samples, currentMarkers, true, null));
				}
				catch (Exception e)
				{
					LOG.error("Error reading asynchronously genotypes for export", e);
					progress.setError("Error reading asynchronously genotypes for export: " + e);
				}
			}
		}.start();
	}
	
	static public abstract class AbstractDataOutputHandler<T, V> implements Callable<Void> {
	    protected V variantDataChunkMap;
	    
	    public void setVariantDataArray(V variantDataArray) {
	        this.variantDataChunkMap = variantDataArray;
	    }

	    public abstract Void call();
	}
	
	protected void launchWritingProcess()
	{
		try
		{
			while ((progress == null || !progress.isAborted()) && (nNumberOfChunks > lastWrittenChunkIndex + 1))
			{
				long b4 = System.currentTimeMillis();
				while (!dataWritingQueue.containsKey(lastWrittenChunkIndex + 1))
					Thread.sleep(20);
				long delay = System.currentTimeMillis() - b4;
				if (lastWrittenChunkIndex > 0 && delay > 100)
					LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing chunk " + (lastWrittenChunkIndex + 1));

				dataOutputHandler.setVariantDataArray(dataWritingQueue.get(lastWrittenChunkIndex + 1));
				dataOutputHandler.call();	// invoke data writing
				dataWritingQueue.remove(++lastWrittenChunkIndex);
				progress.setCurrentStepProgress((lastWrittenChunkIndex + 1) * 100 / nNumberOfChunks);
				
				if ((progress != null && progress.isAborted()) || lastWrittenChunkIndex + 1 >= nNumberOfChunks)
					return;

				new Thread()
				{
					public void run()
					{
						tryAndGetNextChunks();
					}
				}.start();
			}
		}
		catch (Throwable t)
		{
			LOG.error("Error writing export output", t);
			progress.setError("Error writing export output: " + t.getMessage());
		}
	}
}
