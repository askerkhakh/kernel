package ru.sonarplus.kernel.sqlobject.common_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public class CodeValue implements Cloneable {
    // TODO портировать функциональность TCodeValue из Delphi
    public static final char CodeSeparator = '.';
    private byte[] value;

    public byte[] getValue() {
        byte[] res = new byte[this.value.length];
        System.arraycopy(this.value, 0, res, 0, this.value.length);
        return res;
    }

    public CodeValue(byte[] value) {
        Preconditions.checkArgument(value != null && value.length != 0);
        this.value = new byte[value.length];
        System.arraycopy(value, 0, this.value, 0, value.length);
    }

    public static CodeValue valueOf(byte[] value) {
       return new CodeValue(value);
    }

    public static CodeValue valueOf(String value) {
        String[] items = StringUtils.split(value, CodeSeparator);
        byte[] bytes = new byte[items.length];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = Integer.valueOf(items[i]).byteValue();
        return valueOf(bytes);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (byte b: value) {
            if (sb.length() != 0)
                sb.append(CodeSeparator);
            sb.append(Byte.toUnsignedInt(b));
        }
        if (sb.length() == 0)
            sb.append(0);
        return sb.toString();
    }

    public static class Utils {

        public static CodeValue codeRangeLo (CodeValue value, int codeSize) {
            if (value == null)
                return null;
            byte[] result = codeRangeLo(value.getValue(), codeSize);
            return new CodeValue(result);
        }

        public static CodeValue codeRangeHi (CodeValue value, int codeSize) {
            if (value == null)
                return null;
            byte[] result = codeRangeHi(value.getValue(), codeSize);
            return new CodeValue(result);
        }

        public static Integer codeLevel(CodeValue value) {
            if (value == null)
                return null;
            return codeLevel(value.getValue());
        }

        public static byte codeCSB1(CodeValue value) {
            if (value == null)
                return 0;
            return codeCSB1(value.getValue()).byteValue();
        }

        public static final Integer CSB_MAX_VALUE = 254; /*0xFE*/

        public static byte codeCSB2(CodeValue value) {
            if (value == null)
                return 0;
            Integer level = codeLevel(value);
            return (byte) ((CSB_MAX_VALUE - level) & 0xFF);
        }

        public static byte[] codeRangeLo(byte[] value, int codeSize) {
            if (value == null)
                return null;
            Preconditions.checkArgument(codeSize != 0);
            byte[] result = Arrays.copyOf(value, codeSize);
            for (int i = 0; i < result.length; i++) {
                if (result[i] == 0) {
                    result[i] = 1;
                    result = Arrays.copyOf(result, i + 1);
                    break;
                }
            }
            return result;
        }

        public static byte[] codeRangeHi(byte[] value, int codeSize) {
            if (value == null)
                return null;
            Preconditions.checkArgument(codeSize != 0);
            byte[] result = Arrays.copyOf(value, codeSize);
            for (int i = result.length-1; i >= 0 ; i--)
                if (result[i] == 0)
                    result[i] = -1; // 0xFF
                else
                    break;
            return result;
        }

        public static Integer codeLevel(byte[] value) {
            if (value == null)
                return 0;
            for (int i = 0; i < value.length; i++)
                if (value[i] == 0)
                    return i;
            return value.length;
        }

        public static Integer codeCSB1(byte[] value) {
            return codeLevel(value) + 1;
        }

        public static Integer codeCSB2(byte[] value) {
            return CSB_MAX_VALUE - codeLevel(value);
        }

    }

}
