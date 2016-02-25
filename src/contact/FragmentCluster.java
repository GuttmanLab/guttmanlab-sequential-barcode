package contact;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

import guttmanlab.core.annotation.Annotation;

/**
 * A set of fragments with the same barcode sequence
 * Instances are intended to be mutable.
 * @author prussell
 *
 * @param <T> Annotation type that represents the fragments
 * @param <S> Type of underlying AnnotationCollection containing all the fragments
 */
public interface FragmentCluster<T extends Annotation, S extends Collection<T>> {
	
	/**
	 * @return The barcode sequence shared by all the fragments
	 */
	public BarcodeSequence getBarcodes();
	
	/**
	 * Get the number of barcodes in the barcode sequence shared by this cluster
	 * @return Number of barcodes
	 */
	public int getNumBarcodes();
	
	/**
	 * Get a pointer to the mutable collection of locations
	 * Implementations must return a pointer to the underlying collection, not a copy
	 * @return A pointer to the underlying collection of mapped locations of all the fragments
	 */
	public S getLocations();
	
	/**
	 * Get the number of mapped locations in the cluster
	 * @return The number of locations
	 */
	public int getNumLocations();
	
	/**
	 * Add another mapped fragment
	 * Implementations should have already verified that the new fragment contacts the ones already in the collection
	 * @param region New mapped location to add
	 */
	public void addLocation(T region);
	
	/**
	 * Apply a function with no return value to this object, such as a filter that removes some fragments
	 * @param function Function to apply
	 */
	public void apply(Consumer<FragmentCluster<T, S>> function);
	
	/**
	 * Get a string representation of this fragment cluster, e.g. to write to an output file
	 * @param function Function that produces the string representation
	 * @return String representation
	 */
	public String toString(Function<FragmentCluster<T, S>, String> function);
	
}
