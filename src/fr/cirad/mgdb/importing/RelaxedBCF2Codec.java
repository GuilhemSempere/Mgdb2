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

import htsjdk.tribble.TribbleException;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.bcf2.BCFVersion;

/**
 * A specialized BCF2Codec that allows for BCF minor version 2, 
 * which is frequently produced by modern bcftools (v1.10+).
 */
public class RelaxedBCF2Codec extends BCF2Codec {
    protected final static int LATEST_ALLOWED_MINOR_VERSION = 2;
    
    @Override
    protected void validateVersionCompatibility(final BCFVersion supportedVersion, final BCFVersion actualVersion) {
        // Maintain strict Major version check (Must be BCFv2)
        if (actualVersion.getMajorVersion() != ALLOWED_MAJOR_VERSION) {
            throw new TribbleException("BCF2Codec can only process BCF2 files, this file has major version " 
                + actualVersion.getMajorVersion());
        }
        
        // Relaxed Minor version check: Allow 2.1 AND 2.2
        if (actualVersion.getMinorVersion() < ALLOWED_MINOR_VERSION || 
            actualVersion.getMinorVersion() > LATEST_ALLOWED_MINOR_VERSION) {
            
            throw new TribbleException("BCF2Codec version mismatch. Supported: 2.1 to 2." 
                + LATEST_ALLOWED_MINOR_VERSION + ". Found: " 
                + actualVersion.getMajorVersion() + "." + actualVersion.getMinorVersion());
        }
    }
}