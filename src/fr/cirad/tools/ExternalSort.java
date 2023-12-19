/*******************************************************************************
 * 
 * This class is an adaptation of the public domain code initially found at https://github.com/lemire/externalsortinginjava
 * Adjustments were made in order to support progress indicators
 * 
 *******************************************************************************/
package fr.cirad.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The Class ExternalSort.
 */
public class ExternalSort {
	
	/** The defaultmaxtempfiles. */
	static public int DEFAULTMAXTEMPFILES = 1024;
	
    /**
     * This will simply load the file by blocks of lines, then sort them
     * in-memory, and write the result to temporary files that have to be
     * merged later.
     *
     * @param fbr                a BufferedReader (will be closed after execution of this method)
     * @param inputLength        input length
     * @param cmp                string comparator
     * @param progress 			 optional progress indicator that may be fed by this process
     * @return a list of temporary flat files
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static List<File> sortInBatch(BufferedReader fbr, long inputLength, Comparator<String> cmp, ProgressIndicator progress) throws IOException {
        return sortInBatch(fbr, inputLength, cmp, DEFAULTMAXTEMPFILES, Charset.defaultCharset(), null, false, 0, false, progress);
    }

    /**
     * This will simply load the file by blocks of lines, then sort them
     * in-memory, and write the result to temporary files that have to be
     * merged later. You can specify a bound on the number of temporary
     * files that will be created.
     *
     * @param fbr           a BufferedReader (will be closed after execution of this method)
     * @param inputLength   input length
     * @param cmp           string comparator
     * @param maxtmpfiles   maximal number of temporary files
     * @param cs			the cs
     * @param tmpdirectory  location of the temporary files (set to null for default location)
     * @param distinct      Pass <code>true</code> if duplicate lines should be discarded.
     * @param numHeader     number of lines to preclude before sorting starts
     * @param usegzip 		use gzip compression for the temporary files
     * @param progress 		optional progress indicator that may be fed by this process
     * @return a list of temporary flat files
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static List<File> sortInBatch(BufferedReader fbr, long inputLength, Comparator<String> cmp, int maxtmpfiles, Charset cs, File tmpdirectory, boolean distinct, int numHeader, boolean usegzip, ProgressIndicator progress) throws IOException {
            List<File> files = new ArrayList<File>();
            long blocksize = estimateBestSizeOfBlocks(inputLength, maxtmpfiles);// in bytes

            try {
            		long totalWrittenByteCount = 0;
                    List<String> tmplist = new ArrayList<String>();
                    String line = "";
                    try {
                            int counter = 0;
                            mainLoop: while (line != null) {
                                    long currentblocksize = 0;// in bytes
                                    while ((currentblocksize < blocksize)
                                            && ((line = fbr.readLine()) != null)) {
                                            // as  long as  you have enough memory
                                            if (counter < numHeader) {
                                                    counter++;
                                                    continue;
                                            }
                                            tmplist.add(line);
                                            totalWrittenByteCount += line.length();
                                            // ram usage estimation, not very accurate, still more realistic that the simple 2 * String.length
                                            currentblocksize += StringSizeEstimator.estimatedSizeOf(line);
                                            if (progress != null) {
                                                if (progress.getError() != null || progress.isAborted())
                                                    break mainLoop;

                                            	progress.setCurrentStepProgress((short) (totalWrittenByteCount*100/inputLength));
                                            }
                                    }
                                    files.add(sortAndSave(tmplist, cmp, cs, tmpdirectory, distinct, usegzip));                                   
                                    tmplist.clear();
                            }
                    } catch (EOFException oef) {
                            if (tmplist.size() > 0) {
                                    files.add(sortAndSave(tmplist, cmp, cs,
                                            tmpdirectory, distinct, usegzip));
                                    tmplist.clear();
                            }
                    }
            } catch (java.io.IOException ioe) {
            	// it failed: let's cleanup
            	for (File f : files)
            		f.delete();
            	throw ioe;
            } finally {
                    fbr.close();
            }
            return files;
    }
    
    /**
     * Sort a list and save it to a temporary file.
     *
     * @param tmplist       data to be sorted
     * @param cmp           string comparator
     * @param cs            charset to use for output (can use Charset.defaultCharset())
     * @param tmpdirectory  location of the temporary files (set to null for default location)
     * @param distinct      Pass <code>true</code> if duplicate lines should be discarded.
     * @param usegzip		use gzip compression for the temporary files
     * @return the file containing the sorted data
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static File sortAndSave(List<String> tmplist,
            Comparator<String> cmp, Charset cs, File tmpdirectory,
            boolean distinct, boolean usegzip) throws IOException {
            Collections.sort(tmplist, cmp);
            File newtmpfile = File.createTempFile("sortInBatch",
                    "flatfile", tmpdirectory);
            newtmpfile.deleteOnExit();
            OutputStream out = new FileOutputStream(newtmpfile);
            int ZIPBUFFERSIZE = 2048;
            if (usegzip)
                    out = new GZIPOutputStream(out, ZIPBUFFERSIZE) {
                            {
                                    def.setLevel(Deflater.BEST_SPEED);
                            }
                    };
            BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                    out, cs));
            String lastLine = null;
            try {
                    for (String r : tmplist) {
                            // Skip duplicate lines
                            if (!distinct || !r.equals(lastLine)) {
                                    fbw.write(r);
                                    fbw.newLine();
                                    lastLine = r;
                            }
                    }
            } finally {
                    fbw.close();
            }
            return newtmpfile;
    }

	// we divide the file into small blocks. If the blocks
	// are too small, we shall create too many temporary files.
	/**
	 * Estimate best size of blocks.
	 *
	 * @param inputLength the input length
	 * @param maxtmpfiles the maxtmpfiles
	 * @return the long
	 */
	// If they are too big, we shall be using too much memory.
	public static long estimateBestSizeOfBlocks(long inputLength, int maxtmpfiles) {
	        long sizeoffile = inputLength * 2;
	        /**
	         * We multiply by two because later on someone insisted on
	         * counting the memory usage as 2 bytes per character. By this
	         * model, loading a file with 1 character will use 2 bytes.
	         */
	        // we don't want to open up much more than maxtmpfiles temporary
	        // files, better run out of memory first.
	        long blocksize = sizeoffile / maxtmpfiles
	                + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);
	
	        // on the other hand, we don't want to create many temporary files
	        // for naught. If blocksize is smaller than half the free memory, grow it.
	        long freemem = Runtime.getRuntime().freeMemory();
	        if (blocksize < freemem / 2) {
	                blocksize = freemem / 2;
	        }
	        return blocksize;
	}    

    /**
     * This merges a bunch of temporary flat files.
     *
     * @param files             The {@link List} of sorted {@link File}s to be merged.
     * @param outputfile        The output {@link File} to merge the results to.
     * @param cmp               The {@link Comparator} to use to compare {@link String}s.
     * @param cs                The {@link Charset} to be used for the byte to character conversion.
     * @param distinct          Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param append            Pass <code>true</code> if result should append to {@link File} instead of overwrite. Default to be false for overloading methods.
     * @param usegzip           use gzip compression for the temporary files
     * @param progress 			optional progress indicator that may be fed by this process
     * @param totalDataSize		optional parameter for use with the progress indicator
     * @return The number of lines sorted. (P. Beaudoin)
     * @throws IOException Signals that an I/O exception has occurred.
     * @since v0.1.4
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<String> cmp, Charset cs, boolean distinct,
            boolean append, boolean usegzip, ProgressIndicator progress, long totalDataSize) throws IOException {
//            PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
//                    11, new Comparator<BinaryFileBuffer>() {
//                            @Override
//                            public int compare(BinaryFileBuffer i,
//                                    BinaryFileBuffer j) {
//                                    return cmp.compare(i.peek(), j.peek());
//                            }
//                    });
            ArrayList<BinaryFileBuffer> bfbs = new ArrayList<BinaryFileBuffer>();
            for (int i=0; i<files.size(); i++) {
            		File f = files.get(i);
                    final int BUFFERSIZE = 2048;
                    InputStream in = new FileInputStream(f);
                    BufferedReader br;
                    if (usegzip) {
                            br = new BufferedReader(new InputStreamReader(
                                    new GZIPInputStream(in, BUFFERSIZE), cs));
                    } else {
                            br = new BufferedReader(new InputStreamReader(in,
                                    cs));
                    }

                    BinaryFileBuffer bfb = new BinaryFileBuffer(br);
                    bfbs.add(bfb);
            }
            BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile, append), cs));
            int rowcounter = merge(fbw, cmp, distinct, bfbs, progress, totalDataSize);
            for (File f : files) f.delete();
            return rowcounter;
    }
    
    /**
     * This merges several BinaryFileBuffer to an output writer.
     *
     * @param fbw the fbw
     * @param cmp                A comparator object that tells us how to sort the lines.
     * @param distinct                Pass <code>true</code> if duplicate lines should be discarded. (elchetz@gmail.com)
     * @param buffers                Where the data should be read.
     * @param progress 				  optional progress indicator that may be fed by this process
     * @param totalDataSize 				  optional parameter for use with the progress indicator
     * @return The number of lines sorted. (P. Beaudoin)
     * @throws IOException Signals that an I/O exception has occurred.
     */        
    public static int merge(BufferedWriter fbw, final Comparator<String> cmp, boolean distinct, List<BinaryFileBuffer> buffers, ProgressIndicator progress, long totalDataSize) throws IOException {
            PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
                    11, new Comparator<BinaryFileBuffer>() {
                            @Override
                            public int compare(BinaryFileBuffer i,
                                    BinaryFileBuffer j) {
                                    return cmp.compare(i.peek(), j.peek());
                            }
                    });
            for (BinaryFileBuffer bfb: buffers)
                    if(!bfb.empty())
                            pq.add(bfb);
            int rowcounter = 0;
            String lastLine = null;
            try {
            		long nTotalWrittenByteCount = 0;
                    while (pq.size() > 0) {
                            BinaryFileBuffer bfb = pq.poll();
                            String r = bfb.pop();
                            // Skip duplicate lines
                            if (!distinct || !r.equals(lastLine)) {
                                    fbw.write(r);
                                    fbw.newLine();
                                    lastLine = r;
                            }
                            nTotalWrittenByteCount += r.length();
                            ++rowcounter;
                            if (bfb.empty()) {
                                    bfb.fbr.close();
                            } else {
                                    pq.add(bfb); // add it back
                            }
                            if (progress != null) {
                                if (progress.getError() != null || progress.isAborted())
                                    break;

                            	progress.setCurrentStepProgress((short) (nTotalWrittenByteCount*100/totalDataSize));
                            }
                    }
            } finally {
                    fbw.close();
                    for (BinaryFileBuffer bfb : pq)
                            bfb.close();
            }
            return rowcounter;
    }
}

class BinaryFileBuffer {
    public BufferedReader fbr;
    private String cache;
    private boolean empty;

    public BinaryFileBuffer(BufferedReader r)
            throws IOException {
            this.fbr = r;
            reload();
    }

    public boolean empty() {
            return this.empty;
    }

    private void reload() throws IOException {
            try {
                    if ((this.cache = this.fbr.readLine()) == null) {
                            this.empty = true;
                            this.cache = null;
                    } else {
                            this.empty = false;
                    }
            } catch (EOFException oef) {
                    this.empty = true;
                    this.cache = null;
            }
    }

    public void close() throws IOException {
            this.fbr.close();
    }

    public String peek() {
            if (empty())
                    return null;
            return this.cache.toString();
    }

    public String pop() throws IOException {
            String answer = peek();
            reload();
            return answer;
    }
}

/**
 * @author Eleftherios Chetzakis
 * 
 */
class StringSizeEstimator {

	private static int OBJ_HEADER;
	private static int ARR_HEADER;
	private static int INT_FIELDS = 12;
	private static int OBJ_REF;
	private static int OBJ_OVERHEAD;
	private static boolean IS_64_BIT_JVM;

	/**
	 * Private constructor to prevent instantiation.
	 */
	private StringSizeEstimator() {
	}

	/**
	 * Class initializations.
	 */
	static {
		// By default we assume 64 bit JVM
		// (defensive approach since we will get
		// larger estimations in case we are not sure)
		IS_64_BIT_JVM = true;
		// check the system property "sun.arch.data.model"
		// not very safe, as it might not work for all JVM implementations
		// nevertheless the worst thing that might happen is that the JVM is 32bit
		// but we assume its 64bit, so we will be counting a few extra bytes per string object
		// no harm done here since this is just an approximation.
		String arch = System.getProperty("sun.arch.data.model");
		if (arch != null) {
			if (arch.indexOf("32") != -1) {
				// If exists and is 32 bit then we assume a 32bit JVM
				IS_64_BIT_JVM = false;
			}
		}
		// The sizes below are a bit rough as we don't take into account 
		// advanced JVM options such as compressed oops
		// however if our calculation is not accurate it'll be a bit over
		// so there is no danger of an out of memory error because of this.
		OBJ_HEADER = IS_64_BIT_JVM ? 16 : 8;
		ARR_HEADER = IS_64_BIT_JVM ? 24 : 12;
		OBJ_REF = IS_64_BIT_JVM ? 8 : 4;
		OBJ_OVERHEAD = OBJ_HEADER + INT_FIELDS + OBJ_REF + ARR_HEADER;

	}

	/**
	 * Estimates the size of a {@link String} object in bytes.
	 * 
	 * @param s The string to estimate memory footprint.
	 * @return The <strong>estimated</strong> size in bytes.
	 */
	public static long estimatedSizeOf(String s) {
		return (s.length() * 2) + OBJ_OVERHEAD;
	}

}