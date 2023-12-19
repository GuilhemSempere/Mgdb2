package fr.cirad.tools;

import java.util.Collection;

public abstract class AbstractConfigManager {
	public abstract String getPropertyOverridingPrefix();
	public abstract Collection<String> getNonOverridablePropertyNames();
}
