package contact;

import java.util.Collection;

import guttmanlab.core.annotation.Annotation;

/**
 * An interface representing a 2D matrix of fragment contacts
 * Instances are mutable.
 * @author prussell
 *
 * @param <T> Annotation type parameter for the fragments
 * @param <S> AnnotationCollection type parameter for fragment clusters
 */
public interface ContactMatrix<T extends Annotation, S extends Collection<T>> {
	
	/**
	 * Record a new contact between two fragments in the matrix
	 * @param fragment1 One fragment in the contact
	 * @param fragment2 The other fragment in the contact
	 */
	public void addContact(T fragment1, T fragment2);
	
	/**
	 * Record pairwise contacts for all pairs of fragments in a cluster
	 * @param fragmentCluster Fragment cluster
	 */
	public void addContacts(FragmentCluster<T, S> fragmentCluster);
	
	/**
	 * Write the matrix data to a file
	 * @param outFile Output file
	 */
	public void writeMatrix(String outFile);
	
	/**
	 * Write the matrix to an image file
	 * @param outFile Output file
	 */
	public void writeImage(String outFile);
	
}
