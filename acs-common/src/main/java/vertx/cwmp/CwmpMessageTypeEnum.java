package vertx.cwmp;

import dslforumOrgCwmp12.*;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public enum CwmpMessageTypeEnum {
    /**
     * Values
     */
    INFORM(InformDocument.Inform.class.getSimpleName()),
    INFORM_RESPONSE(InformResponseDocument.InformResponse.class.getSimpleName()),
    ADD_OBJECT(AddObjectDocument.AddObject.class.getSimpleName()),
    DELETE_OBJECT(DeleteObjectDocument.DeleteObject.class.getSimpleName()),
    DOWNLOAD(DownloadDocument.Download.class.getSimpleName()),
    UPLOAD(UploadDocument.Upload.class.getSimpleName()),
    REBOOT(RebootDocument.Reboot.class.getSimpleName()),
    FACTORY_RESET(FactoryResetDocument.FactoryReset.class.getSimpleName()),
    SET_PARAMETER_VALUES(SetParameterValuesDocument.SetParameterValues.class.getSimpleName()),
    SET_PARAMETER_VALUES_RESPONSE(SetParameterValuesResponseDocument.SetParameterValuesResponse.class.getSimpleName()),
    GET_PARAMETER_VALUES(GetParameterValuesDocument.GetParameterValues.class.getSimpleName()),
    GET_PARAMETER_VALUES_RESPONSE(GetParameterValuesResponseDocument.GetParameterValuesResponse.class.getSimpleName()),
    SET_PARAMETER_ATTRIBUTES(SetParameterAttributesDocument.SetParameterAttributes.class.getSimpleName()),
    SET_PARAMETER_ATTRIBUTES_RESPONSE(SetParameterAttributesResponseDocument.SetParameterAttributesResponse.class.getSimpleName()),
    GET_PARAMETER_ATTRIBUTES(GetParameterAttributesDocument.GetParameterAttributes.class.getSimpleName()),
    GET_PARAMETER_ATTRIBUTES_RESPONSE(GetParameterAttributesResponseDocument.GetParameterAttributesResponse.class.getSimpleName());

    // Each Enum Value shall has a String attribute
    public String typeString;

    /**
     * Constructor which requires a type string.
     * @param typeString
     */
    private CwmpMessageTypeEnum (String typeString) {
        this.typeString = typeString;
    }

    }
