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

package com.calix.sxa;

/**
 * Common SXA Vertx exceptions.
 */
public class SxaVertxException extends Exception {
    /**
     * Commonly Used Static Exceptions
     */
    public static final SxaVertxException NULL_POINTER = new SxaVertxException("null pointer!");

    /**
     * Construct a CCNG Exception with the specified detail message.  The
     * faultCode will default to null.
     *
     * @param message a description of the exception
     */
    public SxaVertxException(String message) {
        super(message);
    }
}
