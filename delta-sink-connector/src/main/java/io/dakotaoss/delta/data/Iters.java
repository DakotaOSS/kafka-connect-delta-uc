package io.dakotaoss.delta.data;

import io.delta.kernel.utils.CloseableIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Small helpers for building {@link CloseableIterator}s from plain Java collections. */
public final class Iters {

  private Iters() {}

  /** Wrap a plain {@link Iterator} as a {@link CloseableIterator} with a no-op close. */
  public static <T> CloseableIterator<T> closeable(Iterator<T> delegate) {
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public T next() {
        return delegate.next();
      }

      @Override
      public void close() {
        // nothing to release
      }
    };
  }

  /** A {@link CloseableIterator} that yields a single element. */
  public static <T> CloseableIterator<T> singleton(T element) {
    return new CloseableIterator<T>() {
      private boolean consumed = false;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public T next() {
        if (consumed) {
          throw new NoSuchElementException();
        }
        consumed = true;
        return element;
      }

      @Override
      public void close() {
        // nothing to release
      }
    };
  }
}
