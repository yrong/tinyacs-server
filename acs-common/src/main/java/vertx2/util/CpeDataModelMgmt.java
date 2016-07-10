package vertx2.util;

import broadbandForumOrgCwmpDatamodel14.Model;
import broadbandForumOrgCwmpDatamodel14.ModelObject;
import broadbandForumOrgCwmpDatamodel14.ModelParameter;
import broadbandForumOrgCwmpDatamodel14.ReadWriteAccess;
import vertx2.model.CpeDeviceDataModel;
import vertx2.model.CpeDeviceType;
import dslforumOrgCwmp12.GetParameterNamesResponseDocument;
import dslforumOrgCwmp12.ParameterInfoList;
import dslforumOrgCwmp12.ParameterInfoStruct;
import org.apache.xmlbeans.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Vertx;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Project:  ccng-acs
 *
 * This class maintains a static HashMap of all the known CPE device data models.
 *
 * @author: ronyang
 */
public class CpeDataModelMgmt {
    private static final Logger log = LoggerFactory.getLogger(CpeDataModelMgmt.class.getName());

    /**
     * A static TR-098 data model
     */
    public static Model tr098_1_7_DataModel;
    public static CpeDeviceDataModel defaultCpeDeviceDataModel;

    /**
     * Calix 800RSG Device Type
     */
    public static final CpeDeviceType calix800RsgDeviceType = new CpeDeviceType(
            null,
            "Calix",
            "000631",
            null,
            null,
            null,
            null
    );

    /**
     * Static HashMap that keeps all the known data models
     */
    public static ConcurrentHashMap<String, CpeDeviceDataModel> allDataModels =
            new ConcurrentHashMap<String , CpeDeviceDataModel>();

    /**
     * Initialize the Data Model HashMap by querying the MongoDB, and also subscribes to model notifications against
     * the CCNG message bus (namely Redis).
     */
    public static void init(Vertx vertx, String xmlFilePath) {
        /**
         * Init the built-in TR-098 model
         */
        initTr098DataModel(vertx, xmlFilePath + "/tr-098-1-7-full.xml");


        /**
         * Init the built-in Calix 844RG model
         */
        initCalix844RGDataModel(vertx, xmlFilePath + "/Calix-GigaCenter.xml");
    }


    /**
     * Find the data model for a given device type.
     *
     * @param deviceType
     * @return  The data model for this device type, or null.
     */
    public static CpeDeviceDataModel findDataModelByDeviceType(CpeDeviceType deviceType) {
        /**
         * Traverse all the data models
         */
        for (CpeDeviceDataModel model : allDataModels.values()) {
            /**
             * Traverse the device type list of this data model
             */
            for (CpeDeviceType modelDeviceType : model.deviceTypes) {
                if (modelDeviceType.isParent(deviceType)) {
                    // Found it
                    return model;
                }
            }
        }

        // No match, use the default TR-098 model
        return defaultCpeDeviceDataModel;
    }

    /**
     * Add a new data model to the Hash Map
     *
     * @param newModel
     */
    public static void addNewModel(CpeDeviceDataModel newModel) {
        allDataModels.put(newModel.id, newModel);
    }

    /**
     * Delete a data model by its id
     * @param id
     */
    public static void deleteModelById(String id) {
        allDataModels.remove(id);
    }

    /**
     * Static Method to Create a default CPE Data Model Object based on TR-098
     */
    public static void initTr098DataModel(Vertx vertx, String xmlFilePath) {
        try {
            defaultCpeDeviceDataModel = new CpeDeviceDataModel(
                    vertx,
                    "Default",
                    "Default",
                    xmlFilePath);
            tr098_1_7_DataModel = defaultCpeDeviceDataModel.cwmpDataModel;
        } catch (XmlException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Static Method to Create a default Calix 844RG CPE Data Model Object
     */
    public static void initCalix844RGDataModel(Vertx vertx, String xmlFilePath) {
        try {
            CpeDeviceDataModel model = new CpeDeviceDataModel(
                    vertx,
                    "Calix 800RG",
                    "Calix 800RG Data Model",
                    xmlFilePath);
            model.addCpeDeviceType(calix800RsgDeviceType);
            //model.addCpeDeviceType(calix844rgSimDeviceType);

            //Dump the data model to console
            //model.cwmpDataModel.save(System.out);

            addNewModel(model);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (XmlException e) {
            e.printStackTrace();
        }
    }

    /**
     * Find the TR-098 Model Object by Name
     * @param objName
     * @return
     */
    public static ModelObject getTR098ModelObjectByName(String objName) {
        return getObjectByObjName(tr098_1_7_DataModel, objName);
    }


    /**
     * When learning the data model from the CPEs via the "GetParameterNames" RPC method, the "GetParameterNamesResponse"
     * message will only contain the parameter names and a boolean "writable" attribute.
     *
     * We will have to compare the parameter names against the standard TR-09 model, and figure out the diffs.
     *
     * For the objects/parameters that are defined in TR-098, we copy all the object/parameter's attribute over from TR-098.
     *
     * For the objects/parameters that are not defined in TR-098, we leave them wide open for now.
     */
    public static Model getModelByGetParameterNamesResponse(
            GetParameterNamesResponseDocument.GetParameterNamesResponse getParameterNamesResponse) {
        // Start with an empty model
        Model model = Model.Factory.newInstance();

        // Go through the response
        ParameterInfoList paramList = getParameterNamesResponse.getParameterList();
        log.info("Found " + paramList.sizeOfParameterInfoStructArray() + " parameters in the response");
        for (ParameterInfoStruct paramInfo:paramList.getParameterInfoStructArray()) {
            /**
             * Extract parameter name/writable from ParameterInfoStruct
             */
            String name = paramInfo.getName();
            int writable = paramInfo.getWritable()? ReadWriteAccess.INT_READ_WRITE : ReadWriteAccess.INT_READ_ONLY;

            /**
             * Is it an Object (partial path) or a Parameter?
             */
            if (name.charAt(name.length()-1) == '.') {
                /**
                 * Partial Path ends with a '.' which indicates an Object
                 *
                 * Have TR-098 model already defined this object?
                 */
                ModelObject tr098ModelObject = getTR098ModelObjectByName(name);
                if (tr098ModelObject != null) {
                    /**
                     * Yes, this object is defined in TR-098.
                     * Copy the object over.
                     */
                    model.setObjectArray(model.sizeOfObjectArray(), tr098ModelObject);
                } else {
                    /**
                     * No, this object is not defined in TR-098.
                     *
                     * Create a new object:
                     */
                    ModelObject object = model.addNewObject();
                    object.setName(name);
                    object.setAccess(ReadWriteAccess.Enum.forInt(writable));
                    /**
                     * TODO: How do we figure out other attributes of this vendor specific parameter?
                     */
                    log.info("Created a new vendor specific data model object " + name);
                }
            } else {
                /**
                 * This is a full path which indicates a parameter.
                 *
                 * Do we already have a parent object for it?
                 */
                ModelObject parentObj = getObjectByParamName(model, name);
                if (parentObj == null) {
                    log.error("Unable to find parent object for parameter " + name);
                } else {
                    /**
                     * Found the parent object.
                     *
                     * Have TR-098 already defined this parameter?
                     */
                    ModelParameter tr098ModelParameter = getTR098ModelParameterByName(name);
                    if (tr098ModelParameter != null) {
                        // Copy the TR-098 parameter to the new data model
                        model.setParameterArray(model.sizeOfParameterArray(), tr098ModelParameter);
                    } else {
                        // Create a new vendor specific parameter
                        ModelParameter newParam = model.addNewParameter();
                        newParam.setName(paramInfo.getName());
                        newParam.setAccess(ReadWriteAccess.Enum.forInt(writable));

                        /**
                         * TODO: How do we figure out other attributes of this vendor specific parameter?
                         */
                        log.info("Created a new vendor specific data model parameter " + name);
                    }
                }
            }
        }

        return model;
    }

    /**
     * Determine the parent object name for a given parameter name.
     * @param paramName
     * @return
     */
    public static String getParentObjNameByParameterName(String paramName) {
        String modelObjectName = "";
        for (String segment : paramName.substring(0, paramName.lastIndexOf(".")).replace(".", "-").split("-")) {
            try {
                Integer.parseInt(segment);
                // this segment is an instance id
                modelObjectName += ".{i}";
            } catch (NumberFormatException numberFormatException) {
                // this segment is not an instance id
                if (modelObjectName.length() > 0) {
                    modelObjectName += "." + segment;
                } else {
                    modelObjectName += segment;
                }
            }
        }
        modelObjectName += ".";
        return modelObjectName;
    }

    /**
     * Find Model Object by Parameter Name
     * @param paramName
     * @return
     */
    public static ModelObject getObjectByParamName(Model model, String paramName) {
        return getObjectByObjName(model, getParentObjNameByParameterName(paramName));
    }

    /**
     * Find Model Object by Name
     * @param objName
     * @return
     */
    public static ModelObject getObjectByObjName(Model model, String objName) {
        for (ModelObject object : model.getObjectArray()) {
            if (object.getName().equals(objName)) {
                return object;
            }
        }

        return null;
    }

    /**
     * Get the ModelParameter Struct for a given parameter name based on a given data model.
     *
     * @param model
     * @param paramName
     * @return
     */
    public static ModelParameter getModelParameter(Model model, String paramName) {
        /**
         * Get the parameter's parent object in the data model
         */
        ModelObject modelObject = getObjectByParamName(model, paramName);
        if (modelObject == null) {
            log.error("Unable to find parent object for parameter " + paramName);
            return null;
        }

        /**
         * Short Parameter name within the object
         */
        String paramShortName = paramName.substring(paramName.lastIndexOf(".") + 1, paramName.length());
        /*
        log.debug("paramName " + paramName +
                ": modelObjectName " + modelObject.getName() + ", parameterShortName " + paramShortName);
        */

        /**
         * lookup the parameter within the parent object
         */
        for (ModelParameter modelParameter : modelObject.getParameterArray()) {
            if (paramShortName.equals(modelParameter.getName())) {
                return modelParameter;
            }
        }
        log.error("Unable to find parameter " + paramShortName + " within object " + modelObject.getName() + "!");
        return null;
    }

    /**
     * Find the TR-098 Model Parameter by Name
     * @param paramName
     * @return
     */
    public static ModelParameter getTR098ModelParameterByName(String paramName) {
        return getModelParameter(tr098_1_7_DataModel, paramName);
    }

    /**
     * Get the parameter's XML Schema Type for a given parameter name (based on a given data model).
     *
     * @param model
     * @param paramName
     * @return
     */
    public static SchemaType getParamSchemaType(Model model, String paramName) {
        /**
         * Get the ModelParameter Struct from the data model
         */
        ModelParameter modelParameter = getModelParameter(model, paramName);
        if (modelParameter == null) {
            log.debug("Unable to find ModelParameter Struct for " + paramName +
                    "in data model! Treat it as String for now.");
            return XmlString.type;
        }

        broadbandForumOrgCwmpDatamodel14.Syntax syntax = modelParameter.getSyntax();
        if(syntax == null) {
            log.error("Unable to get syntax for " + paramName + "!");
            return XmlString.type;
        }

        if (syntax.isSetString()) {
            return XmlString.type;
        } else if (syntax.isSetBoolean()) {
            return XmlBoolean.type;
        } else if (syntax.isSetInt()) {
            return XmlInt.type;
        } else if (syntax.isSetUnsignedInt()) {
            return XmlUnsignedInt.type;
        } else if (syntax.isSetUnsignedLong()) {
            return XmlUnsignedLong.type;
        } else if (syntax.isSetLong()) {
            return XmlLong.type;
        } else if (syntax.isSetBase64()) {
            return XmlBase64Binary.type;
        } else if (syntax.isSetHexBinary()) {
            return XmlHexBinary.type;
        } else if (syntax.isSetDateTime()) {
            return XmlDateTime.type;
        } else {
            if (syntax.getDataType() != null && syntax.getDataType().getRef() != null) {
                switch (syntax.getDataType().getRef()) {
                    case "Alias":
                    case "IPAddress":
                        return XmlString.type;
                }
            }

            log.error("unknown type in syntax:\n" + syntax.xmlText());
            return XmlString.type;
        }
    }
}
