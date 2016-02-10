package contact;

import java.util.function.Consumer;

import guttmanlab.core.annotation.Annotation;
import guttmanlab.core.annotationcollection.AnnotationCollection;

public abstract class AbstractFragmentCluster<T extends Annotation, S extends AnnotationCollection<T>> implements FragmentCluster<T, S> {
	
	public void apply(Consumer<FragmentCluster<T, S>> function) {
		function.accept(this);
	}

}
