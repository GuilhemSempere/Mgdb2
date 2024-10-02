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

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

/**
 * The Class ProgressIndicator.
 */
public class ProgressIndicator
{
	
	private static final Logger LOG = Logger.getLogger(ProgressIndicator.class);
	
	static private final HashMap<String, ProgressIndicator> progressIndicators = new HashMap<String, ProgressIndicator>();
	
	/** The m_process id. */
	private String m_processId;
	
	/** The current step progress. */
	private long m_currentStepProgress = 0;
	
	/** The current step number. */
	private short m_currentStepNumber = 0;
	
	/** The step labels. */
	private List<String> m_stepLabels = new ArrayList<String>();
	
	/** The error message if any. */
	private String m_error = null;
	
	/** The notification email. */
	private String m_notificationEmail = null;
	
	/** The description. */
	private String m_description = null;
	
	/** whether it was aborted. */
	private boolean m_fAborted = false;
	
	/** whether it is complete. */
	private boolean m_fComplete = false;
	
	/** Whether it supports percentage. */
	private boolean m_fSupportsPercentage = true;

	/** The final message if any. */
	private String m_finalMessage;

	/** The us number format. */
	static private NumberFormat usNumberFormat = NumberFormat.getNumberInstance(Locale.US);

	static private HashSet<String> cancelledRemovals = new HashSet<>();

	/**
	 * Instantiates a new progress indicator.
	 *
	 * @param sProcessId the process id
	 * @param stepLabels the step labels
	 */
	public ProgressIndicator(String sProcessId, String[] stepLabels)
	{
		m_processId = sProcessId;
		m_stepLabels.addAll(Arrays.asList(stepLabels));
//		LOG.debug("ProgressIndicator " + hashCode() + " created for process " + sProcessId);
	}
	
	/**
	 * Sets the percentage enabled.
	 *
	 * @param fEnabled the new percentage enabled
	 */
	public void setPercentageEnabled(boolean fEnabled)
	{
		m_fSupportsPercentage = fEnabled;
	}
	
	/**
	 * Adds the step.
	 *
	 * @param sStepLabel the step label
	 */
	public void addStep(String sStepLabel)
	{
		m_stepLabels.add(sStepLabel);
	}
		
	/**
	 * Gets the step label.
	 *
	 * @param nStepNumber the n step number
	 * @return the step label
	 */
	public String getStepLabel(short nStepNumber)
	{
		return m_stepLabels.get(nStepNumber);
	}

	/**
	 * Gets the process id.
	 *
	 * @return the process id
	 */
	public String getProcessId() {
		return m_processId;
	}

	/**
	 * Gets the current step progress.
	 *
	 * @return the current step progress
	 */
	public long getCurrentStepProgress() {
		return m_currentStepProgress;
	}

	/**
	 * Sets the current step progress.
	 *
	 * @param currentStepProgress the new current step progress
	 */
	public void setCurrentStepProgress(long currentStepProgress) {
		if (m_fSupportsPercentage && (currentStepProgress < 0 || currentStepProgress > 100))
			LOG.warn("Invalid value for currentStepProgress: " + currentStepProgress);
		m_currentStepProgress = currentStepProgress;
	}

	/**
	 * Gets the current step number.
	 *
	 * @return the current step number
	 */
	public short getCurrentStepNumber() {
		return m_currentStepNumber;
	}

	/**
	 * Move to next step.
	 */
	public void moveToNextStep() {
		m_currentStepNumber++;
		if (m_currentStepNumber > m_stepLabels.size())
			LOG.warn("Moving to unexisting step: " + m_currentStepNumber);
		m_currentStepProgress = 0;
	}

	/**
	 * Gets the step count.
	 *
	 * @return the step count
	 */
	public short getStepCount() {
		return (short) m_stepLabels.size();
	}
	
	/**
	 * Gets the error.
	 *
	 * @return the error
	 */
	public String getError() {
		return m_error;
	}

	/**
	 * Sets the error.
	 *
	 * @param error the new error
	 */
	public void setError(String error) {
		m_error = error;
//		remove(2500);
	}
	
	/**
	 * Gets the notification email.
	 *
	 * @return the notification email
	 */
	public String getNotificationEmail() {
		return m_notificationEmail;
	}

	/**
	 * Sets the notification email.
	 *
	 * @param notificationEmail the new notification email
	 */
	public void setNotificationEmail(String notificationEmail) {
		m_notificationEmail = notificationEmail;
	}
	
	/**
	 * Sets the progress description.
	 *
	 * @param description the new progress description
	 */
	public void setProgressDescription(String description) {
		m_description = description;
	}
	
	/**
	 * Gets the progress description.
	 *
	 * @return the progress description
	 */
	public String getProgressDescription() {
		if (m_stepLabels.size() <= m_currentStepNumber)
			return "Please wait...";
		
		if (m_description != null)
			return m_description;

		return m_stepLabels.get(m_currentStepNumber) + "... " + (m_currentStepProgress == 0 ? "" : ((m_fSupportsPercentage ? m_currentStepProgress : usNumberFormat.format(m_currentStepProgress)) + (m_fSupportsPercentage ? "%" : "")));
	}
	
	/**
	 * Abort.
	 */
	public void abort() {
		setAborted(true);
	}
	
	/**
	 * Remove ProgressIndicator from static Map
	 *
	 * @param delay, time to wait before removing (delay needed because there may be several client pages looking at it (when importing for instance))
	 */
	private void remove(int delay)
	{
		new Timer().schedule(new TimerTask() {
		    @Override
		    public void run() {
		    	if (cancelledRemovals.contains(m_processId)) {
		    		LOG.debug("cancelled removal " + (hashCode()  + ": " + getProgressDescription()) + " for process " + m_processId);
		    		cancelledRemovals.remove(m_processId);
		    	}
		    	else {
			    	progressIndicators.remove(m_processId);
			    	LOG.debug("removed " + (hashCode()  + ": " + getProgressDescription()) + " for process " + m_processId);
		    	}
		    }
		}, delay);
	}
	
	/**
	 * Checks for aborted.
	 *
	 * @return true, if successful
	 */
	public boolean isAborted() {
		return m_fAborted;
	}
	
	/**
	 * Mark as complete.
	 */
	public void markAsComplete(String sFinalMessage) {
		this.m_finalMessage = sFinalMessage;
		setComplete(true);
	}
	
	/**
	 * Mark as complete.
	 */
	public void markAsComplete() {
		markAsComplete(null);
	}
	
	/**
	 * Checks if complete.
	 *
	 * @return true, if complete
	 */
	public boolean isComplete() {
		return m_fComplete;
	}

	protected void setAborted(boolean fAborted) {
		m_fAborted = fAborted;
	}
	
	protected void setComplete(boolean fComplete) {
		m_fComplete = fComplete;
	}

	protected void setFinalMessage(String finalMessage) {
		m_finalMessage = finalMessage;
	}

	public String getFinalMessage() {
		return m_finalMessage;
	}
	
	/**
	 * Gets by id.
	 *
	 * @param sProcessID the process id
	 * @return the progress indicator
	 */
	public static ProgressIndicator get(final String sProcessID)
	{
		ProgressIndicator progress = progressIndicators.get(sProcessID);
		if (progress != null && (progress.isComplete() || progress.isAborted() || progress.getError() != null))
	    	progress.remove(2500);	// we don't want to keep them forever

//		LOG.info("returning " + (progress == null ? progress : (progress.hashCode()  + ": " + progress.getProgressDescription())) + " for process " + sProcessID);
		return progress;
	}

	/**
	 * Register progress indicator.
	 *
	 * @param progress the progress
	 */
	public static void registerProgressIndicator(ProgressIndicator progress)
	{
//		LOG.debug("adding " + (progress.hashCode()  + ": " + progress.getProgressDescription()) + " for process " + progress.getProcessId());
		if (progressIndicators.containsKey(progress.getProcessId()))
			cancelledRemovals.add(progress.getProcessId());		// client is re-querying before we had the time to remove this process from our list: cancel its removal otherwise it won't be able to track it
		progressIndicators.put(progress.getProcessId(), progress);
	}

	/**
	 * Insert a step right after the current one
	 * @param sStepLabel
	 */
	public void insertStep(String sStepLabel) {
		m_stepLabels.add(m_currentStepNumber + 1, sStepLabel);
	}
}
