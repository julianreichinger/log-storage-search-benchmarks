package com.dynatrace.index.util;

import static java.lang.String.format;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Locale;
import javax.annotation.Nullable;

public final class MappedBufferUtil {

  @Nullable
  private static final Method CLEAN_BUFFER;
  @Nullable
  private static final Object THE_UNSAFE;

  static {
    Method clean;
    Object theUnsafe;
    try {
      Class<?> unsafeClass = tryGetUnsafe();
      clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
      clean.setAccessible(true);
      Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
      theUnsafeField.setAccessible(true);
      theUnsafe = theUnsafeField.get(null);
    } catch (Exception e) {
      clean = null;
      theUnsafe = null;
    }

    CLEAN_BUFFER = clean;
    THE_UNSAFE = theUnsafe;
  }

  private MappedBufferUtil() {
    // static helper
  }

  /**
   * Tries to unmap the mapped byte-buffer by accessing the "Unsafe" utility via reflection.
   * Yes, this is a bad idea. Sadly, Java does not provide any alternatives to get mapped files working
   * reliably under Windows.
   *
   * <br>WARNING: After unmapping the buffer, any access to it will crash the JVM.
   *
   * @throws UnmapException in case the buffer cannot be unmapped
   */
  public static void unmapBuffer(MappedByteBuffer buffer) {
    if (CLEAN_BUFFER == null || THE_UNSAFE == null) {
      throw new UnmapException("Cannot unmap buffer because the internal JDK API could not be accessed.");
    }

    try {
      CLEAN_BUFFER.invoke(THE_UNSAFE, buffer);
    } catch (Exception e) {
      throw new UnmapException(e, "Unmapping the buffer failed.");
    }
  }

  private static Class<?> tryGetUnsafe() throws ClassNotFoundException {
    try {
      return Class.forName("sun.misc.Unsafe");
    } catch (Exception ex) {
      // try a fall-back in case sun.misc.Unsafe is removed
      return Class.forName("jdk.internal.misc.Unsafe");
    }
  }

  public static final class UnmapException extends RuntimeException {

    @FormatMethod
    public UnmapException(@FormatString String format, Object... args) {
      super(format(Locale.ROOT, format, args), null);
    }

    @FormatMethod
    public UnmapException(Throwable cause, @FormatString String format, Object... args) {
      super(format(Locale.ROOT, format, args), cause);
    }
  }
}
