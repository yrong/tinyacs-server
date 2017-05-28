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

package vertx.cwmp;

import java.io.PrintWriter;

/**
 * CWMP Exceptions
 */
public class CwmpException extends Exception {
    public final static String INVALID_REQUEST = "Invalid Request";

    /**
     * Fault Code
     */
    private long faultCode;

    /**
     * CWMP Version Enum
     */
    private CwmpVersionEnum cwmpVersion = CwmpMessage.DEFAULT_CWMP_VERSION;

    static final long serialVersionUID = -5621384651494307979L;

    /**
     * Construct a CCNG Exception with the specified detail message.  The
     * faultCode will default to null.
     *
     * @param message a description of the exception
     */
    public CwmpException(String message) {
        this(message, CwmpFaultCodes.ACS_REQUEST_DENIED);
    }

    /**
     * Construct a CCNG Exception with the specified detail message and vendor
     * specific faultCode.
     *
     * @param message a description of the exception
     * @param faultCode a string specifying the vendor specific error code
     */
    public CwmpException(String message, long faultCode) {
        super(message);
        this.faultCode = faultCode;
    }

    /**
     * Construct a CCNG Exception with the specified detail message and vendor
     * specific faultCode and CPE's CWMP Version.
     *
     * @param message a description of the exception
     * @param faultCode a string specifying the vendor specific error code
     * @param cwmpVersion   The CWMP Version of the CPE     *
     */
    public CwmpException(String message, long faultCode, CwmpVersionEnum cwmpVersion) {
        super(message);
        this.faultCode = faultCode;
        this.cwmpVersion = cwmpVersion;
    }

    /**
     * Get the Fault code
     *
     * @return a string specifying the fault code
     */
    public long getFaultCode() {
        return this.faultCode;
    }

    /**
     * Get the CPE's CWMP Version
     * @return
     */
    public CwmpVersionEnum getCwmpVersion() {
        return cwmpVersion;
    }
    /**
     * Returns a short description of this CCNG Exception.
     *
     */
    public String toString() {
        return super.toString() + ", Fault ErorCode: " + faultCode;
    }

    /**
     * Prints this CCNG Exception and its stack trace (including the stack trace
     * of the linkedException if it is non-null) to the PrintStream.
     *
     * @param s PrintStream to use for output
     */
    public void printStackTrace( java.io.PrintStream s ) {
        super.printStackTrace(s);
    }

    /**
     * Prints this CCNG Exception and its stack trace (including the stack trace
     * of the linkedException if it is non-null) to <tt>System.err</tt>.
     *
     */
    public void printStackTrace() {
        super.printStackTrace();
    }

    /**
     * Prints this CCNG Exception and its stack trace (including the stack trace
     * of the linkedException if it is non-null) to the PrintWriter.
     *
     * @param s PrintWriter to use for output
     */
    public void printStackTrace(PrintWriter s) {
        super.printStackTrace(s);
    }
}
