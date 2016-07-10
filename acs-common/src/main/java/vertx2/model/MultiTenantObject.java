package vertx2.model;


import vertx2.util.AcsConstants;

/**
 * Project:  CCNG-ACS
 *
 * This class defines the abstract multi-tenant objects which requires orgId as minimum.
 *
 * @author: ronyang
 */
public abstract class MultiTenantObject {

    public static final String FIELD_NAME_ORG_ID = AcsConstants.FIELD_NAME_ORG_ID;
    // The organization ID is required for multi-tenant objects
    public String orgId;

    /**
     * Default Constructor
     */
    public MultiTenantObject() {
    }

    /**
     * Default Constructor
     * @param orgId
     */
    public MultiTenantObject(String orgId) {
        this.orgId = orgId;
    }

    // Setter/Getter
    public String getOrgId() {
        return orgId;
    }

    /**
     * @return  The OrgId as a string.
     */
    @Override
    public String toString() {
        return "org" + orgId + " ";
    }
}
