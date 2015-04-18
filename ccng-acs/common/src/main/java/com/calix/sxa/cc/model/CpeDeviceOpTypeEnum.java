package com.calix.sxa.cc.model;

/**
 * Project:  SXA-CC
 *
 * CPE Device Operation Type Enum
 *
 * @author: jqin
 */
public enum CpeDeviceOpTypeEnum {
    /**
     * Values               Persist     Multi
     *                      Change      Session
     *                      Log         Op
     */
    GetParameterNames       (false,     false),
    GetParameterValues      (false,     false),
    SetParameterValues      (true,      false),
    SetParameterAttributes  (true,      false),
    GetParameterAttributes  (false,     false),
    AddObject               (true,      false),
    DeleteObject            (true,      false),
    Download                (true,      true),
    Reboot                  (true,      true),
    FactoryReset            (true,      true),
    Diagnostics             (true,      true),
    Upload                  (true,      true),
    Unknown                 (true,      true),      // unknown yet, to be determined by Redis query result
    Invalid                 (true,      true);

    // Boolean Indicator that indicates whether this op type shall be persisted into change logs..
    public boolean bPersistChangeLog;

    // Boolean Indicator that indicates whether this op type involves multiple CWMP sessions.
    public boolean bIsMultiSessionOp;

    // Constructor
    private CpeDeviceOpTypeEnum(boolean bPersistChangeLog, boolean bIsMultiSessionOp) {
        this.bIsMultiSessionOp = bIsMultiSessionOp;
        this.bPersistChangeLog = bPersistChangeLog;
    }

    /**
     * Get Device Op Type Enum by String
     *
     * @param typeString
     */
    public static CpeDeviceOpTypeEnum getDeviceOpTypeEnumByString(String typeString) {
        for (CpeDeviceOpTypeEnum opType : values()) {
            if (opType.name().equals(typeString)) {
                return opType;
            }
        }

        return null;
    }
}
