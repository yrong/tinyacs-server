package com.calix.sxa.cc.model;


import broadbandForumOrgCwmpDatamodel14.DocumentDocument;
import com.calix.sxa.VertxUtils;
import org.apache.xmlbeans.XmlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Project:  CCNG-ACS
 *
 * This class defines the CPE Device Models which can be learned via "GetParameterNames" RPC Method, or pre-built with
 * the following TR specs:
 *
 * - tr-106 TR-069 Device:1.1 Root Object definition
 * - tr-098 TR-069 InternetGatewayDevice:1.1 Root Object definition
 *
 * @author: jqin
 */

public class CpeDeviceDataModel extends MultiTenantObject {
    private static final Logger log = LoggerFactory.getLogger(CpeDeviceDataModel.class.getName());

    // An user-friendly Name for this data model
    String name;
    // An user-friendly Description for this data model
    String description;
    // MongoDB Internal Object Id
    public String id;

    // A CPE Device Model may be shared by multiple CPE Device Types
    public LinkedList<CpeDeviceType> deviceTypes;

    /**
     * The actual CPE data model (i.e. TR-098 or a vendor extension based on TR-098)
     *
     * This is presented via a broadbandForumOrgCwmpDatamodel14.DocumentDocument.Document object
     */
    public broadbandForumOrgCwmpDatamodel14.Model cwmpDataModel;

    /**
     * The XmlBeans Data Model Object cannot be easily stored into MongoDB.
     *
     * So we are going to convert it into XML document and store the XML document into the DB.
     */
    private String dataModelXmlString;

    /**
     * Default constructor
     */
    public CpeDeviceDataModel(Vertx vertx, String name, String description, final String xmlFilePath) throws IOException, XmlException {
        this.name = name;
        this.description = description;
        deviceTypes = new LinkedList<CpeDeviceType>();

        /**
         * Read the XML file content asynchronously
         */
        vertx.fileSystem().readFile(
                xmlFilePath,
                new AsyncResultHandler<Buffer>() {
                    public void handle(AsyncResult<Buffer> ar) {
                        if (ar.succeeded()) {
                            try {
                                cwmpDataModel = DocumentDocument.Factory.parse(ar.result().toString())
                                        .getDocument().getModelArray(0);
                                dataModelXmlString = cwmpDataModel.xmlText();
                            } catch (XmlException e) {
                                e.printStackTrace();
                            }
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "File " + xmlFilePath + " does not exist!" + " (" + ar.cause() + ")"));
                        }
                    }
                });
        //cwmpDataModel = DocumentDocument.Factory.parse(new File(xmlFilePath)).getDocument().getModelArray(0);

        /**
         * TODO: Generate MongoDB Id String
         */
        id = name;
    }

    /**
     * Constructor that requires a device type object without actual data model
     */
    public CpeDeviceDataModel(CpeDeviceType cpeDeviceType) {
        deviceTypes = new LinkedList<CpeDeviceType>();
        deviceTypes.add(cpeDeviceType);
        //this.parameterInfoList = parameterInfoList;

        // Determine the data model name by CPE Manufacture/Model
        name = cpeDeviceType.manufacturer + " " + cpeDeviceType.modelName;
    }

    /**
     * Add a new CPE Device type that supports this data model.
     * @param cpeDeviceType
     */
    public void addCpeDeviceType(CpeDeviceType cpeDeviceType) {
        deviceTypes.add(cpeDeviceType);
    }
}
