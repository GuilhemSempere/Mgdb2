/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2026, <CIRAD> <IRD>
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

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class EightKBAlignedHTTPStream extends SeekableStream {
    private final URL url;
    private HttpURLConnection connection;
    private java.io.InputStream inputStream;
    private long position = 0;
    private long contentLength = -1;
    private boolean closed = false;
    
    private int bufferPos = 0;
    private int bufferSize = 0;
    
    public EightKBAlignedHTTPStream(URL url) throws IOException {
        this.url = url;
        openConnection();
    }
    
    private void openConnection() throws IOException {
        if (connection != null) {
            connection.disconnect();
        }
        
        connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(30000);
        connection.setReadTimeout(300000);
        
        if (position > 0) {
            connection.setRequestProperty("Range", "bytes=" + position + "-");
        }
        
        inputStream = new java.io.BufferedInputStream(connection.getInputStream(), 65536);
        
        // Get content length
        String lengthHeader = connection.getHeaderField("Content-Length");
        if (lengthHeader != null) {
            try {
                contentLength = Long.parseLong(lengthHeader);
            } catch (NumberFormatException e) {
                // Ignore
            }
        }
        
        bufferPos = 0;
        bufferSize = 0;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int r = inputStream.read(b, off + total, len - total);
            if (r == -1) return (total == 0) ? -1 : total;
            total += r; // Stay here until len is satisfied!
        }
        this.position += total;
        return total;
    }
    
    @Override
    public long length() {
        return contentLength;
    }
    
    @Override
    public long position() {
        return position;
    }
    
    @Override
    public void seek(long pos) throws IOException {
        if (pos == position) {
            return;
        }
        
        if (pos < position) {
            // Restart from beginning
            position = 0;
            openConnection();
        }
        
        // Skip forward
        long toSkip = pos - position;
        byte[] skipBuffer = new byte[8192];
        while (toSkip > 0) {
            int skipped = read(skipBuffer, 0, (int) Math.min(toSkip, skipBuffer.length));
            if (skipped <= 0) {
                break;
            }
            toSkip -= skipped;
        }
    }
    
    @Override
    public boolean eof() throws IOException {
        if (contentLength > 0) {
            return position >= contentLength;
        }
        
        // Check if more data is available
        if (bufferPos < bufferSize) {
            return false;
        }
        
        // Try to peek
        inputStream.mark(1);
        int b = inputStream.read();
        inputStream.reset();
        return b == -1;
    }
    
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (inputStream != null) {
                inputStream.close();
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    @Override
    public String getSource() {
        return url.toString();
    }
    
    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int result = read(b, 0, 1);
        return result == -1 ? -1 : b[0] & 0xFF;
    }
}