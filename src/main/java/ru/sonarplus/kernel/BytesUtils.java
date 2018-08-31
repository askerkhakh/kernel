package ru.sonarplus.kernel;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public class BytesUtils {

    public static String bytesNotEmptyToHexString(byte[] bytes, boolean allowZero) {
        return bytesToHexString(bytes, false, allowZero);
    }

    public static String bytesToHexString(byte[] bytes) {
        return bytesToHexString(bytes, true, false);
    }

    protected static String bytesToHexString(byte[] bytes, boolean allowEmptyAsZero, boolean allowZero) {
        // [] -> allowEmptyAsZero ? "00" : error
        // [0] -> "00"
        // [1,0] -> allowZero ? "0100" : "01"
        boolean isEmpty = bytes == null || bytes.length == 0;
        Preconditions.checkState(allowEmptyAsZero || !isEmpty);
        if (isEmpty)
            return "00";

        StringBuilder sb = new StringBuilder();
        for (byte b: bytes) {
            if (b == 0 && !allowZero)
                break;
            sb.append(StringUtils.leftPad(Integer.toHexString(b & 0xFF).toUpperCase(), 2, '0'));
        }

        if (sb.length() == 0) {
            sb.append("00");
        }

        return sb.toString();
    }

}
