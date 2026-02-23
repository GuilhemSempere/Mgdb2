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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.CloseableTribbleIterator;
import htsjdk.tribble.FeatureCodec;
import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.FeatureReader;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.readers.PositionalBufferedStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

/**
 * A flexible BCF Reader able to deal with both compressed and uncompressed BCF.
 */
public class FlexibleBCFReader implements FeatureReader<VariantContext> {
    private final URL url;
    private final FeatureCodec<VariantContext, PositionalBufferedStream> codec;
    private VCFHeader header;
    private final boolean isCompressed;
    
    @SuppressWarnings("unchecked")
    public FlexibleBCFReader(URL url, FeatureCodec<VariantContext, ?> codec, boolean isCompressed) throws IOException {
        this.url = url;
        this.codec = (FeatureCodec<VariantContext, PositionalBufferedStream>) codec;
        this.isCompressed = isCompressed;
        
        // Read header with proper decompression
        InputStream fileStream = url.openStream();
        if (isCompressed)
            fileStream = new BlockCompressedInputStream(fileStream);
        PositionalBufferedStream pbs = new PositionalBufferedStream(fileStream);
        
        try {
            PositionalBufferedStream source = this.codec.makeSourceFromStream(pbs);
            FeatureCodecHeader codecHeader = this.codec.readHeader(source);
            this.header = (VCFHeader) codecHeader.getHeaderValue();
        } finally {
            pbs.close();
        }
    }
    
    @Override
    public CloseableTribbleIterator<VariantContext> iterator() throws IOException {
        // Create a new stream for iteration
        InputStream fileStream = url.openStream();
        if (isCompressed)
            fileStream = new BlockCompressedInputStream(fileStream);

        PositionalBufferedStream pbs = new PositionalBufferedStream(fileStream);
        
        // Skip the header in the stream
        PositionalBufferedStream source = codec.makeSourceFromStream(pbs);
        codec.readHeader(source);
        
        return new CloseableTribbleIterator<VariantContext>() {
            private VariantContext nextVariant = null;
            private boolean nextChecked = false;
            private boolean reachedEnd = false;
            
            @Override
            public boolean hasNext() {
                if (nextChecked) {
                    return nextVariant != null;
                }
                
                if (reachedEnd) {
                    return false;
                }
                
                try {
                    nextVariant = codec.decode(source);
                    nextChecked = true;
                    
                    if (nextVariant == null) {
                        reachedEnd = true;
                        return false;
                    }
                    
                    return true;
                } catch (TribbleException e) {
                    // Check if this is an EOF error
                    if (e.getMessage().contains("Invalid block size -1")) {
                        nextVariant = null;
                        nextChecked = true;
                        reachedEnd = true;
                        return false;
                    }
                    throw new RuntimeException("Failed to decode BCF variant", e);
                } catch (IOException e) {
                    nextVariant = null;
                    nextChecked = true;
                    reachedEnd = true;
                    return false;
                }
            }
            
            @Override
            public VariantContext next() {
                if (!nextChecked) {
                    hasNext();
                }
                nextChecked = false;
                
                if (nextVariant == null) {
                    throw new java.util.NoSuchElementException();
                }
                
                return nextVariant;
            }
            
            @Override
            public void close() {
            	pbs.close();
            }
            
            @Override
            public Iterator<VariantContext> iterator() {
                return this;
            }
        };
    }
    
    @Override
    public void close() throws IOException {
        // No persistent resources to close
    }
    
    @Override
    public Object getHeader() {
        return header;
    }
    
    @Override
    public List<String> getSequenceNames() {
        return header.getContigLines().stream()
            .map(line -> line.getID())
            .collect(java.util.stream.Collectors.toList());
    }
    
    @Override
    public boolean isQueryable() {
        return false;
    }
    
    @Override
    public CloseableTribbleIterator<VariantContext> query(String chr, int start, int end) throws IOException {
        throw new UnsupportedOperationException("Query not supported on compressed BCF without index");
    }
}