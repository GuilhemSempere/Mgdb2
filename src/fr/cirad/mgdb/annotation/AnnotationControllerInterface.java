package fr.cirad.mgdb.annotation;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface AnnotationControllerInterface {

	void annotateRun(HttpServletRequest request, HttpServletResponse response, String sModule, String sProject, String sRun, String genomeName) throws Exception;

	Map<String, Object> genomeAnnotationList(HttpServletRequest request, HttpServletResponse response) throws Exception;

	Map<String, Object> installGenomeAnnotation(HttpServletRequest request, HttpServletResponse response, String genomeName, URL genomeURL, String newGenomeID, String newGenomeName) throws Exception;
	
	String getAnnotationMethod();
	
	String getUserInterfaceRootURL() throws MalformedURLException;

	boolean isValid();

}