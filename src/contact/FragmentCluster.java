package contact;

import java.util.function.Consumer;

import guttmanlab.core.annotation.Annotation;
import guttmanlab.core.annotationcollection.AnnotationCollection;

/**
 * A set of fragments with the same barcode sequence
 * Instances are intended to be mutable.
 * @author prussell
 *
 * @param <T> Annotation type that represents the fragments
 * @param <S> Type of underlying AnnotationCollection containing all the fragments
 */
public interface FragmentCluster<T extends Annotation, S extends AnnotationCollection<T>> {
	
	/**
	 * @return The barcode sequence shared by all the fragments
	 */
	public BarcodeSequence getBarcodes();
	
	/**
	 * @return The mapped locations of all the fragments as an AnnotationCollection
	 */
	public S getLocations();
	
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
	
}
