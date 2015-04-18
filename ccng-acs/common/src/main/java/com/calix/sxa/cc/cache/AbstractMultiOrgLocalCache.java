package com.calix.sxa.cc.cache;

import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.cc.model.AcsApiCrudTypeEnum;
import com.calix.sxa.cc.util.AcsConstants;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Project:  SXA-CC
 * 
 * Abstract Local Cache where each org has its own tree map.
 *
 * @author: jqin
 */
public abstract class AbstractMultiOrgLocalCache extends AbstractLocalCache{

    /**
     * A TreeMap that contains all all the child hash maps (one per org).
     *
     * The Index is the orgId.
     */
    public HashMap<String, TreeMap<String, Object>> allPerOrgTreeMaps;

    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public AbstractMultiOrgLocalCache(
            Vertx vertx,
            String crudEventAddress,
            final String dbCollectionName,
            final String cachedObjectType) {
        super(vertx, crudEventAddress, dbCollectionName, cachedObjectType);
        this.allPerOrgTreeMaps = new HashMap();
        this.hashMap = null;
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    public String getIndexString(JsonObject jsonObject) {
        return jsonObject.getString(AcsConstants.FIELD_NAME_ID);
    }

    /**
     * Override the default CRUD Event Handler
     * @return
     */
    @Override
    public Handler<Message<JsonObject>> getCrudEventHandler() {
        return  new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                JsonObject crudEvent = event.body();

                /**
                 * Extract CRUD Type
                 */
                String crudTypeString = crudEvent.getString(AcsConstants.FIELD_NAME_ACS_CRUD_TYPE);
                if (crudTypeString == null) {
                    log.error("No CRUD Type Found! Invalid " + cachedObjectType
                            + " CRUD Event:\n" + crudEvent.encodePrettily());
                    return;
                }
                AcsApiCrudTypeEnum crudType = AcsApiCrudTypeEnum.getCrudTypeEnumByNameString(crudTypeString);
                log.info("Received a " + cachedObjectType + " " + crudType.name() + " Event:\n"
                        + crudEvent.encodePrettily());
                crudEvent.removeField(AcsConstants.FIELD_NAME_ACS_CRUD_TYPE);

                // Get Org Id
                String orgId = crudEvent.getString(AcsConstants.FIELD_NAME_ORG_ID);
                // Get Raw _id string
                String rawId = crudEvent.getString(AcsConstants.FIELD_NAME_ID);
                // Get index value
                String index = crudEvent.getString(AcsConstants.FIELD_NAME_ID);
                TreeMap<String, Object> thisOrg = orgId == null ? null : getPerOrgTreeMap(orgId);

                /**
                 * Check CRUD Type
                 */
                switch (crudType) {
                    case Create:
                    case Update:
                        try {
                            Object pojo = getPojoByJsonObject(crudEvent);
                            if (pojo != null) {
                                thisOrg.put(index, pojo);
                                rawJsonObjectHashMap.put(rawId, convertRawJsonObject(crudEvent));
                            } else {
                                // Skip saving this object in cache
                            }
                        } catch (Exception e) {
                            log.error("Failed to create a new cached object due to exception "
                                    + e.getMessage() + "! CRUD Event Details:\n" + crudEvent.encodePrettily());
                            e.printStackTrace();
                        }
                        break;

                    case Delete:
                        if (index != null) {
                            if (orgId != null && thisOrg != null) {
                                thisOrg.remove(index);
                            } else {
                                /**
                                 * Will have to traverse all per-org hash maps  to find it
                                 */
                                for (String anOrgId : allPerOrgTreeMaps.keySet())  {
                                    TreeMap<String, Object> aPerOrgTreeMap = allPerOrgTreeMaps.get(anOrgId);
                                    if (aPerOrgTreeMap.containsKey(index)) {
                                        log.info("Removing key " + index + " from org " + anOrgId);
                                        orgId = anOrgId;
                                        thisOrg = aPerOrgTreeMap;
                                        aPerOrgTreeMap.remove(index);
                                        break;
                                    }
                                }

                                // Remove this org if empty
                                if (orgId != null && thisOrg != null && thisOrg.size() == 0) {
                                    log.info("Deleting the last " + cachedObjectType + " from org " + orgId);
                                    allPerOrgTreeMaps.remove(orgId);
                                }
                            }
                            rawJsonObjectHashMap.remove(rawId);
                        } else {
                            /**
                             * Bulk delete or delete by orgId+name
                             *
                             * Simply refresh the entire cache
                             */
                            refresh();
                        }
                }
            }
        };
    }

    /**
     * Get an instance of MongoDB Query Result Handler
     * @return
     */
    public VertxMongoUtils.FindHandler getMongoQueryResultHandler() {
        return new MultiOrgMongoQueryResultHandler();
    }

    /**
     * Inner Class for MongoDB Query Result Handler
     */
    public class MultiOrgMongoQueryResultHandler extends VertxMongoUtils.FindHandler {
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            super.handle(jsonObjectMessage);

            // Do nothing if MongoDB timed out
            if (queryResults == null || VertxMongoUtils.FIND_TIMED_OUT.equals(queryResults)) {
                return;
            }

            //log.debug("Refreshing cache with DB query result from collection " + dbCollectionName + "...");
            for (int i = 0; i < queryResults.size(); i++) {
                JsonObject jsonObject = queryResults.get(i);
                String rawId = jsonObject.getString(AcsConstants.FIELD_NAME_ID);
                if (rawId == null) {
                    log.error("Cannot get internal _id string out of Query Result!\n"
                            + jsonObject.encodePrettily());
                } else {
                    tmpHashMap.put(rawId, convertRawJsonObject(jsonObject));
                }
            }

            if (moreExist == false) {
                // Compare treeMap and tmpHashMap

                // Check for unexpected deletion
                for (String index : rawJsonObjectHashMap.keySet()) {
                    if (tmpHashMap.containsKey(index) == false) {
                        log.info("Deleting key " + index + " from cache..");
                        JsonObject toBeDeleted = rawJsonObjectHashMap.get(index);
                        String orgId = toBeDeleted.getString(AcsConstants.FIELD_NAME_ORG_ID);
                        TreeMap<String, Object> anOrg = allPerOrgTreeMaps.get(orgId);

                        // Remove from per-prg hash map
                        if (anOrg != null) {
                            anOrg.remove(getIndexString(toBeDeleted));
                            if (anOrg.size() == 0) {
                                log.info("Deleting the last " + cachedObjectType + " from org " + orgId);
                                allPerOrgTreeMaps.remove(orgId);
                            }
                        }

                        rawJsonObjectHashMap.remove(index);
                    }
                }

                // Check for unexpected creates and updates
                for (String index : tmpHashMap.keySet()) {
                    boolean bSaveDbObject = false;
                    JsonObject dbObject = tmpHashMap.get(index);
                    JsonObject cacheObject = rawJsonObjectHashMap.get(index);

                    if (cacheObject == null) {
                        log.info("Adding key " + index + " to cache..");
                        bSaveDbObject = true;
                    } else {
                        // Replace if changed
                        if(!cacheObject.equals(dbObject)) {
                            log.info("Replacing key " + index + " ..");
                            bSaveDbObject = true;
                        }
                    }

                    if (bSaveDbObject) {
                        // Add new object or Overwrite existing object
                        String orgId = dbObject.getString(AcsConstants.FIELD_NAME_ORG_ID);
                        TreeMap<String, Object> thisOrg = getPerOrgTreeMap(orgId);
                        try {
                            Object pojo = getPojoByJsonObject(dbObject);
                            if (pojo != null) {
                                rawJsonObjectHashMap.put(index, dbObject);
                                thisOrg.put(getIndexString(dbObject), pojo);
                            }
                        } catch (Exception e) {
                            log.error("Failed to convert DB object to a " + cachedObjectType
                                    + " POJO due to exception " + e.getMessage() + "!\n"
                                    + dbObject.encodePrettily());
                            e.printStackTrace();
                        }
                    }
                }

                // Clean up
                tmpHashMap.clear();
            }
        }
    }

    /**
     * Get a comparator if the implementing class want to sort by a custom comparator.
     */
    public Comparator getComparator(){
        return null;
    }

    /**
     * Return a cached object from the per-org hash map
     * @param orgId
     * @param index
     * @param <T>
     * @return
     */
    public <T> T get(String orgId, String index) {
        //dumpAll();
        TreeMap<String, Object> aPerOrgTreeMap = allPerOrgTreeMaps.get(orgId);

        if (aPerOrgTreeMap == null) {
            return null;
        }

        return (T) aPerOrgTreeMap.get(index);
    }

    /**
     * Get all the cached objects for a given org
     * @param orgId
     * @return
     */
    public TreeMap getPerOrgTreeMap(String orgId) {
        TreeMap perOrgTreeMap = allPerOrgTreeMaps.get(orgId);
        if (perOrgTreeMap == null) {
            perOrgTreeMap = new TreeMap(getComparator());
            allPerOrgTreeMaps.put(orgId, perOrgTreeMap);
        }

        return perOrgTreeMap;
    }
}
