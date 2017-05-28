package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import dslforumOrgCwmp12.ParameterValueList;
import dslforumOrgCwmp12.ParameterValueStruct;
import dslforumOrgCwmp12.SetParameterAttributesList;
import dslforumOrgCwmp12.SetParameterAttributesStruct;
import org.apache.xmlbeans.XmlBoolean;
import org.apache.xmlbeans.XmlString;
import org.apache.xmlbeans.XmlUnsignedInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project: cwmp
 *
 * Bootstrap the following Management Server Attributes:
 *
 * - Connection Request Username/Password
 * - Periodical Inform Enable/Interval
 *
 * @author ronyang
 */
public class ManagementServerBootstrap {
    private static final Logger log = LoggerFactory.getLogger(ManagementServerBootstrap.class.getName());

    /**
     * Default Credentials are "admin"/"admin"
     */
    public static final String DEFAULT_USERNAME = "admin";
    public static final String DEFAULT_PASSWORD = "admin";

    /**
     * Default Periodic Inform Settings
     */
    public static final String DEFAULT_PERIODICAL_INFORM_ENABLE = "true";
    public static final String DEFAULT_PERIODICAL_INFORM_INTERVAL = "86400";

    /**
     * Static Parameter Value/Attribute List that contains the default credentials
     */
    public static ParameterValueList PARAM_VALUE_LIST = initParamValueList();
    public static SetParameterAttributesList PARAM_ATTRIBUTE_LIST = initParamAttributeList();

    /**
     * Initialize the Static Param value list
     */
    private static ParameterValueList initParamValueList() {
        ParameterValueList list = ParameterValueList.Factory.newInstance();
        ParameterValueStruct valueStruct = list.addNewParameterValueStruct();
        valueStruct.setName("InternetGatewayDevice.ManagementServer.ConnectionRequestUsername");
        valueStruct.addNewValue().setStringValue(DEFAULT_USERNAME);
        valueStruct.getValue().changeType(XmlString.type);
        valueStruct = list.addNewParameterValueStruct();
        valueStruct.setName("InternetGatewayDevice.ManagementServer.ConnectionRequestPassword");
        valueStruct.addNewValue().setStringValue(DEFAULT_PASSWORD);
        valueStruct.getValue().changeType(XmlString.type);

        valueStruct = list.addNewParameterValueStruct();
        valueStruct.setName("InternetGatewayDevice.ManagementServer.PeriodicInformEnable");
        valueStruct.addNewValue().setStringValue(DEFAULT_PERIODICAL_INFORM_ENABLE);
        valueStruct.getValue().changeType(XmlBoolean.type);
        valueStruct = list.addNewParameterValueStruct();
        valueStruct.setName("InternetGatewayDevice.ManagementServer.PeriodicInformInterval");
        valueStruct.addNewValue().setStringValue(DEFAULT_PERIODICAL_INFORM_INTERVAL);
        valueStruct.getValue().changeType(XmlUnsignedInt.type);

        return list;
    }

    /**
     * Initialize the Static Param Attribute list
     */
    private static SetParameterAttributesList initParamAttributeList() {
        SetParameterAttributesList  list = SetParameterAttributesList.Factory.newInstance();
        SetParameterAttributesStruct attributeStruct = list.addNewSetParameterAttributesStruct();
        attributeStruct.setName("InternetGatewayDevice.ManagementServer.ConnectionRequestUsername");
        attributeStruct.setNotificationChange(true);
        attributeStruct.setNotification(2);
        attributeStruct = list.addNewSetParameterAttributesStruct();
        attributeStruct.setName("InternetGatewayDevice.ManagementServer.ConnectionRequestPassword");
        attributeStruct.setNotificationChange(true);
        attributeStruct.setNotification(2);
        /*
        attributeStruct = list.addNewSetParameterAttributesStruct();
        attributeStruct.setName("InternetGatewayDevice.ManagementServer.PeriodicInformEnable");
        attributeStruct.setNotificationChange(true);
        attributeStruct.setNotification(2);
        attributeStruct = list.addNewSetParameterAttributesStruct();
        attributeStruct.setName("InternetGatewayDevice.ManagementServer.PeriodicInformInterval");
        attributeStruct.setNotificationChange(true);
        attributeStruct.setNotification(2);
         */

        return list;
    }

    /**
     * Enqueue requests.
     *
     * @param session
     */
    public static void start(CwmpSession session)
            throws CwmpException {
        SetParameterValues.start(session, PARAM_VALUE_LIST);
        SetParameterAttributes.start(session, PARAM_ATTRIBUTE_LIST);
    }
}
