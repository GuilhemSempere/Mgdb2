package fr.cirad.tools.mgdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class VariantQueryWrapper {
	private Collection<BasicDBList> bareQueries;	// not referring to projects & runs
	private BasicDBObject projectQueryForVar;	// project & run filter, designed for VariantData structure
	private BasicDBObject projectQueryForVrd;	// project & run filter, designed for VariantRunData structure
	
	public VariantQueryWrapper(Collection<BasicDBList> bareQueries, BasicDBObject projectQueryForVar, BasicDBObject projectQueryForVrd) {
		this.bareQueries = bareQueries;
		this.projectQueryForVar = projectQueryForVar;
		this.projectQueryForVrd = projectQueryForVrd;
	}

	public VariantQueryWrapper() {
		this(new ArrayList<>(), null, null);
	}

	public Collection<BasicDBList> getBareQueries() {
		return bareQueries;
	}
	
	public Collection<BasicDBList> getVariantDataQueries() {
		if (bareQueries.isEmpty() && projectQueryForVar != null)
			return Arrays.asList(new BasicDBList() {{ add(projectQueryForVar); }});
		
		return projectQueryForVar == null ? bareQueries : bareQueries.stream()
                .map(list -> {
                	BasicDBList modifiedList = ((BasicDBList) list.copy());
                	modifiedList.add(0, projectQueryForVar);
                    return modifiedList;
                })
                .collect(Collectors.toList());
	}
	
	public Collection<BasicDBList> getVariantRunDataQueries() {
		if (bareQueries.isEmpty() && projectQueryForVrd != null)
			return Arrays.asList(new BasicDBList() {{ add(projectQueryForVrd); }});

		return projectQueryForVrd == null ? bareQueries : bareQueries.stream()
                .map(list -> {
                	BasicDBList modifiedList = ((BasicDBList) list.copy());
                	modifiedList.add(0, projectQueryForVrd);
                    return modifiedList;
                })
                .collect(Collectors.toList());
	}
}