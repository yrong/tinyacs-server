package vertx2.model;

import vertx2.CcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  SXA-CC
 *
 * @author: ronyang
 */
public enum AcsFileType {
    /**
     * Values
     */
    Image(          "SW/FW Image",              "1 Firmware Upgrade Image",         null),
    LogFile(        "Log File",                 null,                               "2 Vendor Log File"),
    ConfigFile(     "Configuration File",       "3 Vendor Configuration File",      "1 Vendor Configuration File"),
    SipConfigFile(  "SIP Configuration File",   "X 000631 SIP Configuration File",  null),
    Unknown(        null,                       null,                               null);

    // Private logger
    private static final Logger log = LoggerFactory.getLogger(AcsFileType.class.getName());

    // Each File Type Enum shall has a String attribute
    public String typeString;
    // Each File Type Enum shall has another 2 String attributes for the matching TR069 file type strings
    // One for Download and one for upload
    public String tr069DownloadFileTypeString;
    public String tr069UploadFileTypeString;

    /**
     * Constructor which requires a type string.
     * @param typeString
     */
    private AcsFileType (String typeString, String tr069DownloadFileTypeString, String tr069UploadFileTypeString) {
        this.typeString = typeString;
        this.tr069DownloadFileTypeString = tr069DownloadFileTypeString;
        this.tr069UploadFileTypeString = tr069UploadFileTypeString;
    }

    /**
     * Get File Type by string.
     *
     * @param typeString
     * @return
     * @throws CcException
     */
    public static AcsFileType getAcsFileType(String typeString) {
        for (AcsFileType fileType : values()) {
            if (fileType.typeString.equals(typeString)) {
                return fileType;
            }
        }

        log.error("Invalid ACS File Type " + typeString + "!");
        return Unknown;
    }

    /**
     * Get File Type String by File Type Name.
     *
     * @param typeName  ("Image"/"ConfigFile"/"LogFile"
     * @return
     * @throws CcException
     */
    public static String getAcsFileTypeString(String typeName) {
        for (AcsFileType fileType : values()) {
            if (fileType.name().equals(typeName)) {
                return fileType.typeString;
            }
        }

        log.error("Invalid ACS File Type " + typeName + "!");
        return Unknown.name();
    }

    /**
     * Get the ACS File Type by the Device-Op File Type String
     * @param deviceOpFileType
     * @return
     */
    public static AcsFileType getAcsFileTypeByDeviceOpFileType(String deviceOpFileType) {
        if (deviceOpFileType != null) {
            for (AcsFileType fileType : values()) {
                if (fileType.tr069DownloadFileTypeString != null &&
                        fileType.tr069DownloadFileTypeString.equals(deviceOpFileType)) {
                    return fileType;
                }
                if (fileType.tr069UploadFileTypeString != null &&
                        fileType.tr069UploadFileTypeString.equals(deviceOpFileType)) {
                    return fileType;
                }
            }
        }

        return Unknown;
    }
}
