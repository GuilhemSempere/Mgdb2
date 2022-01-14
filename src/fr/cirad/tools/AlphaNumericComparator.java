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

import java.nio.Buffer;
import java.nio.CharBuffer;
import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;

import static java.nio.CharBuffer.wrap;
import static java.util.Objects.requireNonNull;

public class AlphaNumericComparator<T> implements Comparator<T> {

    private final Collator collator;

    /**
     * Creates a comparator that will use lexicographical sorting of the non-numerical parts of the compared strings.
     */
    public AlphaNumericComparator() {
        collator = null;
    }

    /**
     * Creates a comparator that will use locale-sensitive sorting of the non-numerical parts of the compared strings.
     *
     * @param locale
     *         the locale to use
     */
    public AlphaNumericComparator(final Locale locale) {
        this(Collator.getInstance(requireNonNull(locale)));
    }

    /**
     * Creates a comparator that will use the given collator to sort the non-numerical parts of the compared strings.
     *
     * @param collator
     *         the collator to use
     */
    public AlphaNumericComparator(final Collator collator) {
        this.collator = requireNonNull(collator);
    }

    @Override
    public int compare(T t1, T t2) {
    	String s1 = t1.toString();
    	String s2 = t2.toString();
        final CharBuffer b1 = wrap(s1.toCharArray());
        final CharBuffer b2 = wrap(s2.toCharArray());

        while (b1.hasRemaining() && b2.hasRemaining()) {
            moveWindow(b1);
            moveWindow(b2);

            final int result = compare(b1, b2);
            if (result != 0) {
                return result;
            }

            prepareForNextIteration(b1);
            prepareForNextIteration(b2);
        }

        return s1.length() - s2.length();
    }

    private void moveWindow(final CharBuffer buffer) {
        int start = buffer.position();
        int end = buffer.position();
        final boolean isNumerical = isDigit(buffer.get(start));
        while (end < buffer.limit() && isNumerical == isDigit(buffer.get(end))) {
            ++end;
            if (isNumerical && (start + 1 < buffer.limit()) && isZero(buffer.get(start)) && isDigit(buffer.get(end))) {
                ++start; // trim leading zeros
            }
        }

        ((Buffer) buffer).position(start).limit(end);
    }

    private int compare(final CharBuffer b1, final CharBuffer b2) {
        if (isNumerical(b1) && isNumerical(b2)) {
            return compareNumerically(b1, b2);
        }

        return compareAsStrings(b1, b2);
    }

    private boolean isNumerical(final CharBuffer buffer) {
        return isDigit(buffer.charAt(0));
    }

    private boolean isDigit(final char c) {
        if (collator == null) {
            final int intValue = (int) c;
            return intValue >= 48 && intValue <= 57;
        }
        return Character.isDigit(c);
    }

    private int compareNumerically(final CharBuffer b1, final CharBuffer b2) {
        final int diff = b1.length() - b2.length();
        if (diff != 0) {
            return diff;
        }
        for (int i = 0; i < b1.remaining() && i < b2.remaining(); ++i) {
            final int result = Character.compare(b1.charAt(i), b2.charAt(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private void prepareForNextIteration(final CharBuffer buffer) {
        ((Buffer) buffer).position(buffer.limit()).limit(buffer.capacity());
    }

    private int compareAsStrings(final CharBuffer b1, final CharBuffer b2) {
        if (collator != null) {
            return collator.compare(b1.toString(), b2.toString());
        }
        return b1.toString().compareTo(b2.toString());
    }

    private boolean isZero(final char c) {
        return c == '0';
    }

}
