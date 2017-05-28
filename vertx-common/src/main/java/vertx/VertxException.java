/*
 * Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

package vertx;

/**
 * Common SXA Vertx exceptions.
 */
public class VertxException extends Exception {
    /**
     * Commonly Used Static Exceptions
     */
    public static final VertxException NULL_POINTER = new VertxException("null pointer!");

    /**
     * Construct a CCNG Exception with the specified detail message.  The
     * faultCode will default to null.
     *
     * @param message a description of the exception
     */
    public VertxException(String message) {
        super(message);
    }
}
