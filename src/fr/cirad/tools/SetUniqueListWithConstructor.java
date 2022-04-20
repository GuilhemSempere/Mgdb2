package fr.cirad.tools;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections4.list.SetUniqueList;

/**
 * SetUniqueList is the ideal class for holding variants' known allele lists because it forbids duplicates. However Spring-Data needs it to have a parameter-less constructor for automatic mapping.
 *
 * @author SEMPERE
 */

@SuppressWarnings("serial")
public class SetUniqueListWithConstructor<E> extends SetUniqueList<E> {
    public SetUniqueListWithConstructor() {
        super(new ArrayList<E>(), new HashSet<E>());
    }

    public SetUniqueListWithConstructor(List<E> list) {
        super(list, new HashSet<E>(list));  // we have to call a constructor, these contents will be overwritten by the next line  ;-(
    }
}
