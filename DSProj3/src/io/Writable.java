package io;

import java.io.Serializable;

public interface Writable<T> extends Serializable {
	public T get();
	
	public void set(T t);
	
	public void parse(String s);
}
