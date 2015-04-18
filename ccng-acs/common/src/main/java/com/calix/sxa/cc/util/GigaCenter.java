package com.calix.sxa.cc.util;

import com.calix.sxa.cc.CcException;
import com.calix.sxa.cc.model.AcsFile;
import com.calix.sxa.cc.model.Cpe;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Project:  SXA-CC_PRE-CA_Rel1
 *
 * Calix GigaCenter Specific Tricks/Utils.
 *
 * @author: jqin
 */
public class GigaCenter {
    private static final Logger log = LoggerFactory.getLogger(GigaCenter.class.getName());

    public static Map<String, String> tr098toGigaCenterPathMap = initTr098toGigaCenterPathMap();

    /**
     * Product Classes
     */
    public static final String PRODUCT_CLASS_ONT = "ONT";
    public static final String PRODUCT_CLASS_ENT = "ENT";

    /**
     * Zero-Touch Credentials
     */
    public static final String ZERO_TOUCH_ACTIVATION_PASSWORD = "activate-cxnk";

    /**
     * Some Well Known GigaCenter Parameter Names/Paths
     */
    public static final String CHANGE_COUNTER =
            Cpe.INTERNET_GATEWAY_DEVICE_ROOT + ".DeviceInfo.X_000631_IGDChangeCounter";

    /**
     * Convert a TR-098 parameter path to GigaCenter Path.
     *
     * @param tr098Path
     * @return
     */
    public static String convertTr098Path(String tr098Path) {
        String gigaCenterPath = tr098toGigaCenterPathMap.get(tr098Path);

        if (gigaCenterPath != null) {
            log.info("Converting " + tr098Path + " to " + gigaCenterPath + "...");
            return gigaCenterPath;
        } else {
            return tr098Path;
        }
    }

    /**
     * Initialize the Hash Map with all the special conversions.
     */
    public static Map<String, String> initTr098toGigaCenterPathMap() {
        Map<String, String> map = new HashMap<>();

        // WIFI KeyPassPhrase
        for (int i = 1; i < 16; i ++) {
            map.put(
                    "InternetGatewayDevice.LANDevice.1.WLANConfiguration." + i + ".KeyPassphrase",
                    "InternetGatewayDevice.LANDevice.1.WLANConfiguration." + i + ".PreSharedKey.1.KeyPassphrase"
            );
        }

        return map;
    }

    /**
     * Parse a GigaCenter Image File Header and return the Metadata.
     *
     *  @see <a href="http://wiki.calix.local/display/PREM/800G+image+format">800G Image Format</a>
     *
     * @return Image Metadata as a JSON Object.
     */
    private static final int MIN_IMAGE_HEADER_SIZE = 24;
    private static final int IMAGE_SIGNATURE = 0xCB010000;
    private static final int IMAGE_HEADER_CODE_VERSION = 0x00000001;
    private static final int IMAGE_HEADER_CODE_PAYLOAD_ENCRYPTION_KEY = 0x00000002;
    private static final int IMAGE_HEADER_CODE_KERNEL_ROOT_FS = 0x00000003;
    private static final int IMAGE_HEADER_CODE_SUPPORTED_MODULES = 0x00000004;;
    private static final int SUPPORTED_MODULE_ID_844G_1 = 99;
    private static final int SUPPORTED_MODULE_ID_844G_2 = 100;
    private static final int SUPPORTED_MODULE_ID_854G_1 = 101;
    private static final int SUPPORTED_MODULE_ID_854G_2 = 102;
    private static final int SUPPORTED_MODULE_ID_844E_1 = 103;
    private static final int SUPPORTED_MODULE_ID_844E_2 = 104;

    private static final CcException INVALID_HEADER = new CcException("Invalid Header (null pointer or too short!");
    public static JsonObject getImageMetadata(Buffer buffer) throws CcException {
        if (buffer == null || buffer.length() < MIN_IMAGE_HEADER_SIZE) {
            throw INVALID_HEADER;
        }

        /**
         * The first 16 bytes are the Fixed block header:
         * Offset 0x0000 (4 bytes): Signature, 0xCB, 0x01, 0x00, 0x00
         * Offset 0x0004 (4 bytes): Big-endian 32-bit encoded Image Size in number of bytes, size for the entire image
         *                          file
         * Offset 0x0008 (4 bytes): Big-endian 32-bit encoded Header Size in number of bytes, size of fixed block header
         *                          plus variable size header, same as payload offset
         * Offset 0x000C (4 bytes): Reserved, all zeros
         */
        int signature = buffer.getInt(0);
        if (signature == IMAGE_SIGNATURE) {
            log.info("Found proper image signature: 0x" + String.format("%08X", IMAGE_SIGNATURE));
        } else {
            String error = "Invalid Image Signature (0x" + String.format("%08X", signature) + ")!";
            throw new CcException(error);
        }
        int payloadOffset = buffer.getInt(8);

        /**
         * Variable size header, containing a group of variable size header fields:
         *
         * Image Version (code 1):
         * Code (4 bytes): 0x00, 0x00, 0x00, 0x01
         * Length (4 bytes): Big-endian 32-bit encoded header field length in number of bytes (4)
         * Image version (16 bytes): ASCII version string with padded zeros
         *
         * Let us read all headers one at a time.
         */
        JsonObject metadata = new JsonObject();
        int offset = 16;
        while (offset <= (buffer.length() - 8) && offset < payloadOffset) {
            /**
             * Read Header Code
             */
            int code = buffer.getInt(offset);
            offset += 4;
            int headerSize = buffer.getInt(offset);
            offset += 4;
            if ((buffer.length() - offset) >= headerSize) {
                byte[] header;
                switch (code) {
                    case IMAGE_HEADER_CODE_VERSION:
                        header = buffer.getBytes(offset, offset + headerSize);
                        String version = new String(header);
                        for (int i = 0; i < headerSize; i ++) {
                            // Version String is terminated by '\0'
                            if (header[i] == 0) {
                                version = version.substring(0, i);
                                break;
                            }
                        }
                        log.info("version: " + version);
                        metadata.putString(AcsFile.FIELD_NAME_VERSION, version);
                        break;

                    case IMAGE_HEADER_CODE_PAYLOAD_ENCRYPTION_KEY:
                    case IMAGE_HEADER_CODE_KERNEL_ROOT_FS:
                        break;

                    case IMAGE_HEADER_CODE_SUPPORTED_MODULES:
                        header = buffer.getBytes(offset, offset + headerSize);
                        JsonArray supportedModules = new JsonArray();
                        for (int i = offset; i < (offset + headerSize); i +=4) {
                            int aSupportedModuleId = buffer.getInt(i);
                            switch (aSupportedModuleId) {
                                case SUPPORTED_MODULE_ID_844G_1:
                                    supportedModules.add("844G-1");
                                    break;
                                case SUPPORTED_MODULE_ID_844G_2:
                                    supportedModules.add("844G-2");
                                    break;
                                case SUPPORTED_MODULE_ID_854G_1:
                                    supportedModules.add("854G-1");
                                    break;
                                case SUPPORTED_MODULE_ID_854G_2:
                                    supportedModules.add("854G-2");
                                    break;
                                case SUPPORTED_MODULE_ID_844E_1:
                                    supportedModules.add("844E-1");
                                    break;
                                case SUPPORTED_MODULE_ID_844E_2:
                                    supportedModules.add("844E-2");
                                    break;
                                default:
                                    log.error("Unknown Support Module ID " + aSupportedModuleId + "!");
                                    break;
                            }
                            metadata.putArray(AcsFile.FIELD_NAME_MODELS, supportedModules);
                        }
                        log.info("Supported Modules: " + supportedModules);
                        break;

                    default:
                        log.error("Unknown Image Header Code 0x" + String.format("%08X", code) + "!");
                        break;
                        //throw INVALID_HEADER;
                }
                offset += headerSize;
                log.info("code: " + String.format("%08X", code)
                        + ", size: " + String.format("%08X", headerSize)
                        + ", offset: " + offset);
            } else {
                log.error("Buffer Overflow!");
            }
        }

        log.info("Image Metadata:\n" + metadata.encodePrettily());
        return metadata;
    }

    /**
     * Parse a GigaCenter Config File and return the Metadata.
     *
     * Format of the R11.1 Calix Proprietary Header:
     * <!--CalixVersion=”1.2.3.4” crc32=”1234abcd” type="golden" fsan="CXNK00181234" -->
     *
     * @param buffer
     *
     * @return Config File Metadata as a JSON Object.
     */
    public static final String CALIX_CONFIG_FILE_HEADER_FIELD_VERSION = "CalixVersion";
    public static final String CALIX_CONFIG_FILE_HEADER_FIELD_CRC32 = "crc32";
    public static final String CALIX_CONFIG_FILE_HEADER_FIELD_TYPE = "type";
    public static final String CALIX_CONFIG_FILE_HEADER_FIELD_FSAN = "fsan";
    public static final String CALIX_CONFIG_FILE_HEADER_PREFIX = "<!--" + CALIX_CONFIG_FILE_HEADER_FIELD_VERSION;
    public static JsonObject getConfigFileMetadata(Buffer buffer) throws CcException {
        if (buffer == null) {
            throw INVALID_HEADER;
        }

        /**
         * The first line could be the Calix Proprietary Header
         */
        String headerLine = new String(buffer.getBytes(0, 256));
        if (!headerLine.startsWith(CALIX_CONFIG_FILE_HEADER_PREFIX)) {
            log.info("Not an R11.1 Config File (no Calix Proprietary Header found)!");
            return null;
        }

        int headerEnd = headerLine.indexOf("-->");
        if (headerEnd < 0) {
            log.info("Invalid Calix Proprietary Header " + headerLine + "!");
            return null;
        }

        /**
         * parse the header
         */
        String header = headerLine.substring(headerLine.indexOf(CALIX_CONFIG_FILE_HEADER_FIELD_VERSION), headerEnd);
        JsonObject metadata = new JsonObject();

        String[] fields = StringUtil.split(header, ' ');
        for (String aField : fields) {
            if (!aField.contains("=")) {
                continue;
            }

            String fieldName = aField.substring(0, aField.indexOf("="));
            String value = aField.substring(aField.indexOf("=") + 2, aField.lastIndexOf("\""));
            log.info("Processing field " + fieldName + ": " + value);
            switch (fieldName) {
                case CALIX_CONFIG_FILE_HEADER_FIELD_VERSION:
                    metadata.putString(AcsFile.FIELD_NAME_VERSION, value);
                    break;

                case CALIX_CONFIG_FILE_HEADER_FIELD_TYPE:
                case CALIX_CONFIG_FILE_HEADER_FIELD_FSAN:
                    break;

                case CALIX_CONFIG_FILE_HEADER_FIELD_CRC32:
                    metadata.putString(AcsFile.FIELD_NAME_CRC32, value);
            }
        }

        log.info("Image Metadata:\n" + metadata.encodePrettily());

        /**
         * TODO: Validate CRC32 if any
         */
        if (metadata.containsField(AcsFile.FIELD_NAME_CRC32)) {
            //String crc32 = metadata.getString(AcsFile.FIELD_NAME_CRC32);
        }

        return metadata;
    }

    /**
     * Check if the given username is a valid zero-touch username ([OUI]-[ProductClass]-[FSAN]).
     * @param username
     * @return
     */
    public static boolean isZeroTouchUsername(String username) {
        String[] fields = StringUtil.split(username, '-');
        if (fields == null || fields.length != 3) {
            return false;
        }

        if (!fields[0].equals(Cpe.CALIX_OUI)) {
            return false;
        }

        if (!fields[1].equals(PRODUCT_CLASS_ONT) && !fields[1].equals(PRODUCT_CLASS_ENT)) {
            return false;
        }

        if (!AcsMiscUtils.isFSANString(fields[2])) {
            return false;
        }

        return true;
    }
}