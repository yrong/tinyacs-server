package vertx2.model;

/**
 * Project:  cwmp
 *
 * ACS API CRUD Operation Type Enums.
 *
 */
public enum AcsApiCrudTypeEnum {
    /**
     * Values
     */
    Create("POST"),
    Retrieve("GET"),
    Update("PUT"),
    Delete("DELETE"),
    Unknown("UNKNOWN");


    public String httpMethodString;

    /**
     * Constructor
     */
    private AcsApiCrudTypeEnum(String httpMethodString) {
        this.httpMethodString = httpMethodString;
    }

    /**
     * Get CRUD Type Enum by HTTP Method String
     * @param httpMethodString
     * @return
     */
    public static AcsApiCrudTypeEnum getCrudTypeEnumByHttpMethodString(String httpMethodString) {
        for (AcsApiCrudTypeEnum crudTypeEnum : values()) {
            if (crudTypeEnum.httpMethodString.equals(httpMethodString)) {
                return crudTypeEnum;
            }
        }

        return null;
    }

    /**
     * Get CRUD Type Enum by name String
     * @param nameString
     * @return
     */
    public static AcsApiCrudTypeEnum getCrudTypeEnumByNameString(String nameString) {
        for (AcsApiCrudTypeEnum crudTypeEnum : values()) {
            if (crudTypeEnum.name().equals(nameString)) {
                return crudTypeEnum;
            }
        }

        return null;
    }
}
