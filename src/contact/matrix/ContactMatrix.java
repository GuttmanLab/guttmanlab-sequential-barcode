package contact.matrix;

import java.io.File;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

import contact.FragmentCluster;
import Jama.Matrix;
import guttmanlab.core.annotation.Annotation;

/**
 * Class to construct and output a matrix representing contacts between genomic loci
 * Contacts are inferred from {@link FragmentCluster}s
 * Implementations should include a data structure for storing all contacts
 * A 2D matrix can be constructed on the fly where cells represent contacts between genomic bins
 * Instances are mutable
 * 
 * @author prussell
 *
 * @param <T> Annotation type for fragment clusters
 * @param <S> Collection type for fragment clusters
 * @param <U> Type of summary of each location to be stored in data structure
 */
public abstract class ContactMatrix<T extends Annotation, S extends Collection<T>, U extends Comparable<U>> {
	
	/**
	 * Record a new contact between two locations
	 * @param location1summary Summary of one location
	 * @param location2summary Summary of other location
	 */
	public abstract void addContact(U location1summary, U location2summary);
		
	/**
	 * Add all contacts represented by a {@link FragmentCluster} to the data structure
	 * Only adds contacts on one side of the diagonal
	 * @param fragmentCluster A collection of mapped fragments whose locations are assumed to contact each other
	 * @param summarize A function to summarize each fragment of type {@link T} for inclusion in the data structure
	 */
	public void addContacts(FragmentCluster<T, S> fragmentCluster, Function<T, U> summarize) {
		for(T fragment1 : fragmentCluster.getLocations()) {
			U fragment1summary = summarize.apply(fragment1);
			for(T fragment2 : fragmentCluster.getLocations()) {
				U fragment2summary = summarize.apply(fragment2);
				if(fragment1summary.compareTo(fragment2summary) < 0) 
					addContact(fragment1summary, fragment2summary);
			}
		}
	}
	
	/**
	 * Get a matrix representation of the contacts currently stored in the data structure
	 * @param binSizeAndLocationToBin Function accepting a bin size and location summary, and returning the bin number for the location
	 * @return Matrix where each cell contains a representation of all contacts between the two bins
	 */
	public abstract Matrix getMatrix(BiFunction<Integer, U, Integer> binSizeAndLocationToBin);
	
	/**
	 * Write matrix to a file
	 * @param outFile File to write to
	 */
	public abstract void writeMatrix(File outFile);
	
	/**
	 * Write image to a file
	 * @param outFile File to write to
	 */
	public abstract void writeImage(File outFile);
	
}
