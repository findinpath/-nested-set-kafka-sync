package com.findinpath.source;

public class Utils {
    private Utils(){}

    public static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}
