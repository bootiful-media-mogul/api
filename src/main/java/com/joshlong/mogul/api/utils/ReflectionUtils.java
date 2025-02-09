package com.joshlong.mogul.api.utils;

import org.springframework.core.ResolvableType;

import java.util.HashSet;
import java.util.Set;

public abstract class ReflectionUtils {

	public static Set<Class<?>> genericsFor(Class<?> clzz) {
		var classes = new HashSet<Class<?>>();
		genericsFor(clzz, classes);
		return classes;
	}

	private static void genericsFor(Class<?> clzz, Set<Class<?>> classes) {
		var resolvableType = ResolvableType.forClass(clzz);

		// Get direct generics of this class
		for (var generic : resolvableType.getGenerics()) {
			if (generic.resolve() != null) {
				classes.add(generic.resolve());
				// Recurse into the generic's type parameters
				genericsFor(generic.resolve(), classes);
			}
		}

		// Get generics from all implemented interfaces
		for (var iface : resolvableType.getInterfaces()) {
			for (ResolvableType generic : iface.getGenerics()) {
				if (generic.resolve() != null) {
					classes.add(generic.resolve());
					genericsFor(generic.resolve(), classes);
				}
			}
		}

		// Get generics from superclass
		var superType = resolvableType.getSuperType();
		if (!superType.equals(ResolvableType.NONE)) {
			for (ResolvableType generic : superType.getGenerics()) {
				if (generic.resolve() != null) {
					classes.add(generic.resolve());
					genericsFor(generic.resolve(), classes);
				}
			}
		}
	}

}
