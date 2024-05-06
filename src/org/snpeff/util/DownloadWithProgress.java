package org.snpeff.util;

import fr.cirad.tools.ProgressIndicator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;

public class DownloadWithProgress extends Download {
	private static final int BUFFER_SIZE = 102400;

	protected ProgressIndicator progress;
	
	public DownloadWithProgress(ProgressIndicator progress) {
		this.progress = progress;
	}

	@Override
	public boolean download(URL url, String localFile) {
	    boolean res = false;
	    try {
	        sslSetup(); // Set up SSL for websites having issues with certificates (e.g. Sourceforge)
	
	        if (verbose) Log.info("Connecting to " + url);
	
	        URLConnection connection = openConnection(url);
	
	        // Follow redirect? (only for http connections)
	        if (connection instanceof HttpURLConnection) {
	            for (boolean followRedirect = true; followRedirect; ) {
	                HttpURLConnection httpConnection = (HttpURLConnection) connection;
	                if (verbose) Log.info("Connecting to " + url + ", using proxy: " + httpConnection.usingProxy());
	                int code = httpConnection.getResponseCode();
	
	                if (code == 200) {
	                    followRedirect = false; // We are done
	                } else if (code == 302) {
	                    String newUrl = connection.getHeaderField("Location");
	                    if (verbose) Log.info("Following redirect: " + newUrl);
	                    url = new URL(newUrl);
	                    connection = openConnection(url);
	                } else if (code == 404) {
	                    throw new RuntimeException("File not found on the server. Make sure the database name is correct.");
	                } else throw new RuntimeException("Error code from server: " + code);
	            }
	        }
	
	        // Copy resource to local file, use remote file if no local file name specified
	        InputStream is = connection.getInputStream();
	
	        // Print info about resource
	        Date date = new Date(connection.getLastModified());
	        if (debug) Log.debug("Copying file (type: " + connection.getContentType() + ", size: " + connection.getContentLengthLong() + ", modified on: " + date + ")");
	
	        // Open local file
	        if (verbose) Log.info("Local file name: '" + localFile + "'");
	
	        // Create local directory if it doesn't exists
	        File file = new File(localFile);
	        if (file != null && file.getParent() != null) {
	            File path = new File(file.getParent());
	            if (!path.exists()) {
	                if (verbose) Log.info("Local path '" + path + "' doesn't exist, creating.");
	                path.mkdirs();
	            }
	        }
	
	        FileOutputStream os = null;
	        os = new FileOutputStream(localFile);
	
	        int progressPercentage = 0;
	        // Copy to file
	        int count = 0, total = 0, lastShown = 0;
	        byte[] data = new byte[BUFFER_SIZE];
	        while ((count = is.read(data, 0, BUFFER_SIZE)) != -1) {
	            os.write(data, 0, count);
	            total += count;
	            
	            int newProgressPercentage = (int) (total * 100d / connection.getContentLengthLong());
	            if (newProgressPercentage != progressPercentage) {
	            	progressPercentage = newProgressPercentage;
	            	progress.setCurrentStepProgress(progressPercentage);
	            }
	
	            // Show every MB
	            if ((total - lastShown) > (1024 * 1024)) {
	                if (verbose) System.err.print(".");
	                lastShown = total;
	            }
	        }
	        if (verbose) Log.info("");
	
	        // Close streams
	        is.close();
	        os.close();
	        if (verbose) Log.info("Download finished. Total " + total + " bytes.");
	        progress.markAsComplete();
	
	        res = true;
	    } catch (Exception e) {
	        res = false;
	        if (verbose) Log.info("ERROR while connecting to " + url);
	        if (!maskDownloadException) throw new RuntimeException(e);
	    }
	
	    return res;
	}
}
