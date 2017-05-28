package vertx.model;

/**
 * Project:  CCNG-ACS
 *
 * This class defines the data model for the Manufacturer Objects.
 *
 * Each Manufacture object shall include the following attributes:
 * - Organization Id
 * - Manufacturer Name
 * - Manufacturer OUI String
 *
 * @author: ronyang
 */

// The @Entity Annotation defines the MongoDB collection name for this Java Class.
public class Manufacturer  extends MultiTenantObject {
    // User Name String is the ID field
    String name;

    // OUI String
    String oui;
    @Override
    /**
     * Custom toString() method.
     */
    public String toString() {
        return "org" + getOrgId() + ":" +name +": OUI " + oui;
    }
}
