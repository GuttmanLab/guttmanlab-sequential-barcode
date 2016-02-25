package contact;

import java.util.Collection;
import java.util.function.Consumer;

import guttmanlab.core.annotation.Annotation;

public abstract class AbstractFragmentCluster<T extends Annotation, S extends Collection<T>> implements FragmentCluster<T, S> {
	
	public void apply(Consumer<FragmentCluster<T, S>> function) {
		function.accept(this);
	}
	
	public int getNumLocations() {
		return getLocations().size();
	}
	
	public int getNumBarcodes() {
		return getBarcodes().getNumBarcodes();
	}
		
}
