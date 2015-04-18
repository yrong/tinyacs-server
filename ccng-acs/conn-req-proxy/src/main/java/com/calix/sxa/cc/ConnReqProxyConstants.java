package com.calix.sxa.cc;

import com.calix.sxa.VertxUtils;

/**
 * Project:  SXA-CC
 *
 * @author: jqin
 */
public class ConnReqProxyConstants {

    /**
     * HTTP Service Port (default to 8181)
     */
    public static final int DEFAULT_HTTP_SERVICE_PORT = 8181;
    public static final String HTTP_SERVICE_PORT_SYS_ENV_VAR = "SXA_CC_CONN_REQ_HTTP_SERVICE_PORT";
    public static final int HTTP_SERVICE_PORT  = VertxUtils.initIntegerProp(HTTP_SERVICE_PORT_SYS_ENV_VAR, DEFAULT_HTTP_SERVICE_PORT);
}
