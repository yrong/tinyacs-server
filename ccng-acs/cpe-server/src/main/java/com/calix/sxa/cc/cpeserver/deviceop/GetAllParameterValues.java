package com.calix.sxa.cc.cpeserver.deviceop;

import broadbandForumOrgCwmpDatamodel14.ModelObject;
import com.calix.sxa.cc.cpeserver.session.CwmpSession;
import com.calix.sxa.cc.model.CpeDeviceDataModel;
import dslforumOrgCwmp12.ParameterNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  ccng-acs
 *
 * @author: jqin
 */
public class GetAllParameterValues {
    private static final Logger log = LoggerFactory.getLogger(GetAllParameterValues.class.getName());

    /**
     * Send a "GetParameterValues" RPC method to the CPE with the root object name + "."
     *
     * @param session
     */
    public static void start(CwmpSession session)  {
        GetAllParameterValues.start(session, null);
    }

    /**
     * Get all parameter values for a CPE that supports a known data model.
     *
     * We will specify the partial paths of the all the child objects of the root object
     *
     * @param session
     * @param model
     */
    public static void start(CwmpSession session, CpeDeviceDataModel model)  {
        ParameterNames paramNames = ParameterNames.Factory.newInstance();
        if (model != null) {
            for (ModelObject object : model.cwmpDataModel.getObjectArray()) {
                paramNames.addString(object.getName().replace("{i}", "1"));
            }
        } else {
            paramNames.addString(session.cpe.rootObjectName + ".");
        }
        GetParameterValues.start(session, paramNames);
    }
}
