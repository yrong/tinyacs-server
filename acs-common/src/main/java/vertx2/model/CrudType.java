package vertx2.model;

/**
 * Project:  SXA-CC
 *
 * CRUD Operation Type Enums.
 *
 */
public enum CrudType {
    /**
     * Enum Values
     */
    Create,
    Retrieve,
    Update,
    Delete,
    Unknown;

    /**
     * Get CRUD Type Enum by String.
     *
     * @param crudTypeString
     * @return  A CRUD Type Enum, or null.
     */
    public static CrudType getCrudTypeByString(String crudTypeString) {
        for (CrudType crudType : values()) {
            if (crudType.name().equals(crudTypeString)) {
                return crudType;
            }
        }

        return null;
    }
}
