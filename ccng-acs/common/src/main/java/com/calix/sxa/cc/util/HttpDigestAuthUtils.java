package com.calix.sxa.cc.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.auth.AUTH;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.impl.Base64;

import java.security.SecureRandom;

/**
 * Project:  SXA-CC
 *
 * HTTP Digest Auth Utils (both server and client)
 *
 * @author: jqin
 */
public class HttpDigestAuthUtils {
    private static final Logger log = LoggerFactory.getLogger(HttpDigestAuthUtils.class);

    /**
     * Generate Digest Response String based on the challenge string and username/password/uri.
     *
     * @param challengeString
     * @param uri
     * @param method
     * @param username
     * @param password
     * @return
     */
    private static int nonceCount = 0;
    public static String getAuthResponse(
            String challengeString,
            String uri,
            String method,
            String username,
            String password) {
        // Initialize the auth response header with the challenge string
        Header authHeader = new BasicHeader(AUTH.WWW_AUTH_RESP, challengeString.substring(7));
        String nonce = getHeaderElementValue(authHeader, "nonce");
        String opaque = getHeaderElementValue(authHeader, "opaque");
        String realm = getHeaderElementValue(authHeader, "realm");
        String qop = getHeaderElementValue(authHeader, "qop");

        // Generate cnonce
        String cnonce = getRandomNonce();

        // Increase nc
        nonceCount ++;
        String nc = String.format("%08X", nonceCount);

        // Calculate response using password/nonce/cnonce/etc
        String a1 = username + ":" + realm + ":" + password;
        String md5a1 = DigestUtils.md5Hex(a1);
        String a2 = method + ":" + uri;
        String md5a2 = DigestUtils.md5Hex(a2);
        String a3 = md5a1 + ":" + nonce + ":" + nc + ":" + cnonce + ":" + qop + ":" + md5a2;
        String response = DigestUtils.md5Hex(a3);

        return "Digest username=\"" + username
                + "\", realm=\"" + realm
                + "\", nonce=\"" + nonce
                + "\", uri=\"" + uri
                + "\", cnonce=\"" + cnonce
                + "\", nc=\"" + nc
                + "\", qop=\"auth"
                + "\", response=\"" + response
                + "\", opaque=\"" + opaque
                + "\", algorithm=\"MD5\"";
    }

    /**
     * Get a random nonce string.
     */
    public static String getRandomNonce() {
        final SecureRandom rnd = new SecureRandom();
        final byte[] tmp = new byte[8];
        rnd.nextBytes(tmp);
        return Base64.encodeBytes(tmp);
    }

    /**
     * Get value of an element from Header.
     *
     * @param header
     * @param elementName
     */
    public static String getHeaderElementValue(Header header, String elementName) {
        for (HeaderElement headerElement : header.getElements()) {
            if (headerElement.getName().equals(elementName)) {
                return headerElement.getValue();
            }
        }

        return null;
    }

    /**
     * Get an Auth Challenge String.
     */
    public static String getChallengeString(String nonce, String domain) {
        return "Digest realm=\"" + AcsConstants.SXA_CC_AUTH_REALM + "\", "
                + "domain=\"" + domain + "\", "
                + "algorithm=MD5, qop=\"auth\", nonce=\"" + nonce + "\", "
                + "opaque=\"" + DigestUtils.md5Hex(nonce) + "\"";
    }

    /**
     * Extract the username from Auth Header.
     */
    public static String getUsernameFromAuthHeader(String authHeader) {
        if (authHeader == null || authHeader.length() < 10) {
            log.error("Invalid Auth header " + authHeader);
            return null;
        }

        /**
         * Parse Auth Header
         */
        // trim the leading "Digest " prefix
        String trimmedAuthResp = authHeader.substring(7);
        Header authRespHeader = new BasicHeader(AUTH.WWW_AUTH_RESP, trimmedAuthResp);
        return getHeaderElementValue(authRespHeader, "username");
    }

    /**
     * Authenticate the auth header received.
     *
     * @param authHeader    The Auth Header received from client
     * @param nonceSent     The nonce string sent before within the challenge string
     * @param username
     * @param password
     *
     * @return true if successfully verified; or false
     */
    public static boolean verifyAuthHeader(
            String authHeader,
            String nonceSent,
            String username,
            String password) {
        if (authHeader == null || authHeader.length() < 10) {
            log.error("Invalid Auth header " + authHeader);
            return false;
        }

        /**
         * Parse Auth Header
         */
        // trim the leading "Digest " prefix
        String trimmedAuthResp = authHeader.substring(7);
        Header authRespHeader = new BasicHeader(AUTH.WWW_AUTH_RESP, trimmedAuthResp);
        String usernameFromClient = getHeaderElementValue(authRespHeader, "username");
        String realm = getHeaderElementValue(authRespHeader, "realm");
        String nonceFromClient = getHeaderElementValue(authRespHeader, "nonce");
        String nc = getHeaderElementValue(authRespHeader, "nc");
        String cnonce = getHeaderElementValue(authRespHeader, "cnonce");
        String qop = getHeaderElementValue(authRespHeader, "qop");
        String uri = getHeaderElementValue(authRespHeader, "uri");
        String response = getHeaderElementValue(authRespHeader, "response");
        //String opaque = getHeaderElementValue(authRespHeader, "opaque");

        // Validate Username if any
        if (!username.equals(usernameFromClient)) {
            log.error("Invalid username " + usernameFromClient + "!");
            return false;
        }

        // Validate Realm
        if (!AcsConstants.SXA_CC_AUTH_REALM.equals(realm)) {
            log.error("Invalid realm " + realm + "!");
            return false;
        }

        if (nonceFromClient == null || nc == null || cnonce == null || qop == null) {
            log.error("Missing nonce or nc or cnonce or qoq! (" + authHeader + ")");
            return false;
        }

        if (!nonceFromClient.equals(nonceSent)) {
            log.error("Nonce Mismatch! (" + nonceFromClient + " vs. " + nonceSent + ")");
            return false;
        }

        // Calculate expected response using password/nonce/cnonce/etc
        String a1 = username + ":" + realm + ":" + password;
        String md5a1 = DigestUtils.md5Hex(a1);
        String a2 = "POST:" + uri;
        String md5a2 = DigestUtils.md5Hex(a2);
        String a3 = md5a1 + ":" + nonceSent + ":" + nc + ":" + cnonce + ":" + qop + ":" + md5a2;
        String expectedResponse = DigestUtils.md5Hex(a3);

        if (!expectedResponse.equals(response)) {
            log.error("Incorrect Digest Response! Expecting: " +
                    expectedResponse + ", actual response: " + response);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Generate nonce with orgId.
     */
    public static String getNonceByOrgId(String orgId) {
        return "SXA-CC~" + orgId;
    }
}
