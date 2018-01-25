package kr.co.vcnc.haeinsa;

import java.nio.ByteBuffer;

public class ByteBufferUtils {
  public static byte[] getByteArray(ByteBuffer buffer) {
    byte[] array = new byte[buffer.remaining()];
    buffer.slice().get(array);
    return array;
  }
}
