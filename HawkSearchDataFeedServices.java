package com.hotwaxmedia.hawksearch;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import freemarker.template.TemplateException;

import org.apache.commons.io.FileUtils;
import org.ofbiz.base.util.Debug;
import org.ofbiz.base.util.template.FreeMarkerWorker;
import org.ofbiz.base.util.UtilDateTime;
import org.ofbiz.base.util.UtilMisc;
import org.ofbiz.base.util.UtilValidate;
import org.ofbiz.entity.Delegator;
import org.ofbiz.entity.GenericEntity;
import org.ofbiz.entity.GenericEntityException;
import org.ofbiz.entity.GenericValue;
import org.ofbiz.entity.condition.EntityCondition;
import org.ofbiz.entity.condition.EntityConditionList;
import org.ofbiz.entity.condition.EntityExpr;
import org.ofbiz.entity.condition.EntityOperator;
import org.ofbiz.entity.transaction.GenericTransactionException;
import org.ofbiz.entity.transaction.TransactionUtil;
import org.ofbiz.entity.util.EntityListIterator;
import org.ofbiz.entity.util.EntityQuery;
import org.ofbiz.entity.util.EntityUtil;
import org.ofbiz.entity.util.EntityUtilProperties;
import org.ofbiz.product.product.ProductContentWrapper;
import org.ofbiz.service.*;

import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.transaction.Transaction;


public class HawkSearchDataFeedServices {
    public static final String module = HawkSearchDataFeedServices.class.getName();

    public static Map<String, Object> prepareItemsDataFeed(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();
        Locale locale = (Locale) context.get("locale");
        Map<String, Object> dataFeedMap = new HashMap<String, Object>();
        String outputDirLocation = (String) context.get("outputDirLocation");
        String fileName = (String) context.get("fileName");
        String templateLocation = (String) context.get("templateLocation");
        boolean isHeader = true;
        int totalItems = 0;
        Timestamp lastFeedTimestamp = (Timestamp) context.get("lastFeedTimestamp");
        GenericValue userLogin = (GenericValue) context.get("userLogin");
        List<Map<String, Object>> itemInfoList = new LinkedList<Map<String, Object>>();
        Map<String, Object> itemDataFeeds = new HashMap<String, Object>();
        Timestamp nowTimestamp = UtilDateTime.nowTimestamp();
        Transaction parentTx = null;
        Transaction defaultParentTx = null;
        Transaction runningTx = null;

        String includeProducts = EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch.ALL_PRODUCTS", delegator);
        String endPoint = EntityUtilProperties.getPropertyValue("AWSS3Credentials", "awss3.endpoint", delegator);
        Map<String, Object> itemInfoMap = null;
        EntityListIterator productFeatureAndAppls = null;

        // Declaring the global set of variables to decrease heap memory consumption for large data sets.
        BigDecimal retailPrice;
        GenericValue goodCore = null;
        GenericValue jobberCore = null;
        GenericValue jsonProdCtntAndInfo = null;
        GenericValue product = null;
        GenericValue subBrand = null;
        GenericValue wdCore = null;

        List<BigDecimal> priceList;

        String brandCode;
        String brandName;
        String imageUrl;
        String marketDesc;
        String name;
        String partNumber = "";
        String productId = "";
        String subBrandCode;
        String terminologyName;

        StringBuilder descriptionShort;
        String descriptionLong;
        StringBuilder sku;
        try {
            File file = new File(outputDirLocation + "/" + fileName);
            //Writer writer = new BufferedWriter(new FileWriter(file, true));
            Charset cs = Charset.forName("UTF-8");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), cs));

            EntityCondition eligibleProductCond = EntityCondition.makeCondition(UtilMisc.toList(
                EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_7"),
                EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_8"),
                EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_9"),
                EntityCondition.makeCondition("statusId", EntityOperator.EQUALS, null)
            ), EntityOperator.OR);

            EntityCondition modifiedProductCond = EntityCondition.makeCondition("isHawkIndexed", EntityOperator.EQUALS, "Y");
            EntityCondition globalProductCondition = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.OR, modifiedProductCond);
            
            EntityCondition cond = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("productTypeId", EntityOperator.EQUALS, "FINISHED_GOOD"),
                    EntityCondition.makeCondition("domainPartyId", EntityOperator.EQUALS, "AAPA"),
                    EntityCondition.makeCondition("primaryProductCategoryId", EntityOperator.NOT_EQUAL, null)
            ), EntityOperator.AND);
            globalProductCondition = EntityCondition.makeCondition(globalProductCondition, EntityOperator.AND, cond);
            if (UtilValidate.isNotEmpty(includeProducts) && "N".equals(includeProducts)) {
                EntityCondition dateCondition = EntityCondition.makeCondition(UtilMisc.toList(
                        EntityCondition.makeCondition("lastModifiedDate", EntityOperator.GREATER_THAN, lastFeedTimestamp),
                        EntityCondition.makeCondition("productUpdateDate", EntityOperator.GREATER_THAN, lastFeedTimestamp)
                ), EntityOperator.OR);
                globalProductCondition = EntityCondition.makeCondition(globalProductCondition, EntityOperator.AND, dateCondition);
            }
            EntityCondition disDateCondition = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("salesDiscontinuationDate", EntityOperator.EQUALS, null),
                    EntityCondition.makeCondition("salesDiscontinuationDate", EntityOperator.GREATER_THAN_EQUAL_TO, nowTimestamp)
            ), EntityOperator.OR);

            globalProductCondition = EntityCondition.makeCondition(globalProductCondition, EntityOperator.AND, disDateCondition);
            defaultParentTx = TransactionUtil.suspend();//Suspend T0 - Default Transaction
            try {
                TransactionUtil.begin();  //Begin T1
            } catch (GenericTransactionException gte) {
                Debug.logError(gte, "Unable to begin transaction T1", module);
            }
            productFeatureAndAppls = EntityQuery.use(delegator).from("HawkConfiguredProducts").where(globalProductCondition).orderBy("aaiaBrandId").fetchSize(Integer.MIN_VALUE).queryIterator();
            parentTx = TransactionUtil.suspend(); //Suspend T1
            try {
                TransactionUtil.begin();  //Begin T2
            } catch (GenericTransactionException gte) {
                Debug.logError(gte, "Unable to begin transaction T2", module);
            }
            if (productFeatureAndAppls != null) {
                GenericValue productFeatureAndAppl = null;
                while ((productFeatureAndAppl = productFeatureAndAppls.next()) != null) {
                    Debug.logInfo("Part Number | ProductId [" + productFeatureAndAppl.getString("partNumber") + " | " + productFeatureAndAppl.getString("productId") + "]", module);
                    itemInfoMap = new HashMap<String, Object>();
                    productId = productFeatureAndAppl.getString("productId");
                    partNumber = productFeatureAndAppl.getString("partNumber");
                    if (UtilValidate.isEmpty(partNumber)) {
                        Debug.logError("Part Number is not found for the product : " + productId, module);
                        continue;
                    }
                    if (UtilValidate.isEmpty(productFeatureAndAppl.getString("allianceProductId"))) {
                        Debug.logError("Alliance Product ID is not found for the product : " + productId, module);
                        continue;
                    }

                    product = EntityQuery.use(delegator).from("Product").where("productId", productId).queryOne();
                    // Item Operation
                    if (UtilValidate.isNotEmpty(includeProducts) && "Y".equals(includeProducts)) {
                        //Debug.logInfo("Include all products set as Y", module);
                        if (UtilValidate.isEmpty(lastFeedTimestamp) || UtilValidate.isEmpty(productFeatureAndAppl.getString("isHawkIndexed"))
                                || "N".equals(productFeatureAndAppl.getString("isHawkIndexed"))) {
                            if (!("PRODUCT_STS_7".equals(productFeatureAndAppl.getString("statusId"))) || !("PRODUCT_STS_8".equals(productFeatureAndAppl.getString("statusId"))) ||
                                    !("PRODUCT_STS_9".equals(productFeatureAndAppl.getString("statusId"))) || productFeatureAndAppl.getString("statusId") == null) {
                                itemInfoMap.put("item_operation", "A");
                                if (product != null) {
                                    product.set("isHawkIndexed", "Y");
                                    product.set("hawkIndexedDate", nowTimestamp);
                                    product.store();
                                }
                            } else {
                                Debug.logError("The product is not eligible for Hawk data feed " + productId, module);
                                continue;
                            }
                        } else {
                            if (UtilValidate.isNotEmpty(lastFeedTimestamp) && "Y".equals(productFeatureAndAppl.getString("isHawkIndexed"))) {
                                if (!("PRODUCT_STS_7".equals(productFeatureAndAppl.getString("statusId"))) || !("PRODUCT_STS_8".equals(productFeatureAndAppl.getString("statusId"))) ||
                                        !("PRODUCT_STS_9".equals(productFeatureAndAppl.getString("statusId"))) || productFeatureAndAppl.getString("statusId") == null) {
                                    itemInfoMap.put("item_operation", "U");
                                } else {
                                    itemInfoMap.put("item_operation", "D");
                                    if (product != null) {
                                        product.set("isHawkIndexed", "N");
                                        product.set("hawkIndexedDate", nowTimestamp);
                                        product.store();
                                    }
                                }
                            }
                        }
                    } else {
                    if (UtilValidate.isEmpty(lastFeedTimestamp) || UtilValidate.isEmpty(productFeatureAndAppl.getString("isHawkIndexed"))
                            || "N".equals(productFeatureAndAppl.getString("isHawkIndexed"))) {
                        if (!("PRODUCT_STS_7".equals(productFeatureAndAppl.getString("statusId"))) || !("PRODUCT_STS_8".equals(productFeatureAndAppl.getString("statusId"))) ||
                                !("PRODUCT_STS_9".equals(productFeatureAndAppl.getString("statusId"))) || productFeatureAndAppl.getString("statusId") == null) {
                            itemInfoMap.put("item_operation", "A");
                            if (product != null) {
                                product.set("isHawkIndexed", "Y");
                                product.set("hawkIndexedDate", nowTimestamp);
                                product.store();
                            }
                        } else {
                            Debug.logError("The product is not eligible for Hawk data feed " + productId, module);
                            continue;
                        }
                    } else {
                        if (UtilValidate.isNotEmpty(lastFeedTimestamp) && "Y".equals(productFeatureAndAppl.getString("isHawkIndexed")) && UtilValidate.isNotEmpty(productFeatureAndAppl.getTimestamp("lastModifiedDate")) && lastFeedTimestamp.before(productFeatureAndAppl.getTimestamp("lastModifiedDate")) || UtilValidate.isNotEmpty(productFeatureAndAppl.getTimestamp("productUpdateDate")) && lastFeedTimestamp.before(productFeatureAndAppl.getTimestamp("productUpdateDate"))) {
                            if (!("PRODUCT_STS_7".equals(productFeatureAndAppl.getString("statusId"))) || !("PRODUCT_STS_8".equals(productFeatureAndAppl.getString("statusId"))) ||
                                    !("PRODUCT_STS_9".equals(productFeatureAndAppl.getString("statusId"))) || productFeatureAndAppl.getString("statusId") == null) {
                                itemInfoMap.put("item_operation", "U");
                            } else {
                                itemInfoMap.put("item_operation", "D");
                                if (product != null) {
                                    product.set("isHawkIndexed", "N");
                                    product.set("hawkIndexedDate", nowTimestamp);
                                    product.store();
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                    }

                    itemInfoMap.put("unique_id", productFeatureAndAppl.getString("allianceProductId"));
                    itemInfoMap.put("sort_default", EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch." + productFeatureAndAppl.getString("nationalPopularityCode"), "70000000", delegator));

                    //Code to generate terminology name.
                    name = "";
                    ProductContentWrapper allianceProductName = new ProductContentWrapper(dispatcher, product, locale, "text/plain");
                    if (UtilValidate.isNotEmpty(allianceProductName) && UtilValidate.isNotEmpty(allianceProductName.get("PRODUCT_NAME", "string"))) {
                        name = allianceProductName.get("PRODUCT_NAME", "string").toString();
                        if (UtilValidate.isNotEmpty(name)) {
                            itemInfoMap.put("name", formatContentForCsv(name));
                        }
                    }
                    
                    terminologyName = "";
                    GenericValue terminology = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", productFeatureAndAppl.getString("primaryProductCategoryId")).queryOne();
                    if (terminology != null && UtilValidate.isNotEmpty(terminology.getString("description"))) {
                        terminologyName = terminology.getString("description");
                    }

                    // thumbnail
                    imageUrl = getProductThumbnail(delegator, productId);
                    if (imageUrl != null) {
                        itemInfoMap.put("image", imageUrl);
                    }

                    //Code to generate brand code and brand name.
                    brandName = "";
                    brandCode = null;
                    GenericValue brand = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", productFeatureAndAppl.getString("aaiaBrandId")).queryOne();
                    if (brand != null) {
                        brandName = brand.getString("description");
                        brandCode = brand.getString("categoryName");
                    }
                    //Code to generate sub brand code.
                    subBrandCode = "";
                    subBrand = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", productFeatureAndAppl.getString("aaiaSubBrandId")).queryOne();
                    if (subBrand != null) {
                        subBrandCode = subBrand.getString("categoryName");
                    }

                    //Code to generate the JSON Bucket URL.
                    jsonProdCtntAndInfo = EntityQuery.use(delegator).from("ProductContentAndInfo").where("productId", productId, "productContentTypeId", "JSON_DOCUMENT").filterByDate().queryFirst();
                    if (jsonProdCtntAndInfo != null) {
                        itemInfoMap.put("url_detail", endPoint + jsonProdCtntAndInfo.getString("drObjectInfo"));
                    }

                    //Code to generate short description.
                    descriptionShort = new StringBuilder(brandName);
                    if (UtilValidate.isNotEmpty(terminologyName)) {
                        descriptionShort = descriptionShort.append(" ").append(terminologyName);
                    itemInfoMap.put("description_short", formatContentForCsv(descriptionShort.toString()));

                    }
                    //Code to get the market description for the product

                    marketDesc = allianceProductName.get("MARKETING_DESC", "string").toString();
                    descriptionLong = "";
                    //Code to generate long description.
                    if (UtilValidate.isNotEmpty(marketDesc)) {
                        descriptionLong = marketDesc;
                    }

                    //Code to generate the sku.
                    sku = new StringBuilder();
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("stdPartNumber"))) {
                        sku = sku.append(productFeatureAndAppl.getString("stdPartNumber")).append("-");
                    } else {
                        sku = sku.append("-");
                    }
                    if (UtilValidate.isNotEmpty(brandCode)) {
                        sku = sku.append(brandCode).append("-");
                    } else {
                        sku = sku.append("-");
                    }
                    if (UtilValidate.isNotEmpty(subBrandCode)) {
                        sku = sku.append(subBrandCode);
                    }

                    itemInfoMap.put("sku", sku);
                    if (UtilValidate.isNotEmpty(descriptionLong)) {
                    itemInfoMap.put("description_long", formatContentForCsv(descriptionLong.toString()));

                    }
                    // Retail Price
                    retailPrice = getProductRetailPrice(delegator, productId);
                    if (retailPrice != null) {
                    itemInfoMap.put("retailPrice", retailPrice);
                    }
                    // Price Special Calculation
                    priceList = new ArrayList<BigDecimal>();
                    goodCore = EntityQuery.use(delegator).from("ProductPrice").where("productId", productId, "productPriceTypeId", "CRG").filterByDate().queryFirst();
                    if (goodCore != null){
                        priceList.add(goodCore.getBigDecimal("price"));
                    }

                    // Jobber Core

                    jobberCore = EntityQuery.use(delegator).from("ProductPrice").where("productId", productId, "productPriceTypeId", "CRJ").filterByDate().queryFirst();
                    if (jobberCore != null){
                        priceList.add(jobberCore.getBigDecimal("price"));
                    }

                    // Wd Core
                    wdCore = EntityQuery.use(delegator).from("ProductPrice").where("productId", productId, "productPriceTypeId", "CRW").filterByDate().queryFirst();
                    if (wdCore != null){
                        priceList.add(wdCore.getBigDecimal("price"));
                    }

                    // Maximum value for the above calculated prices.
                    if (UtilValidate.isNotEmpty(priceList)) {
                        itemInfoMap.put("priceSpecial", Collections.max(priceList));
                    }
                    itemInfoList.add(itemInfoMap);

                    if (itemInfoList.size() % 1000 == 0)     {
                        totalItems = totalItems + itemInfoList.size();
                        dataFeedMap.put("dataFeedList", (List) itemInfoList);
                        dataFeedMap.put("isHeader", isHeader);
                        FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                        itemInfoList.clear();
                        isHeader = false;
                        TransactionUtil.commit();  //Commit T2
                        
                        runningTx = TransactionUtil.suspend(); //Suspend T2

                        TransactionUtil.resume(defaultParentTx); //Resume T1

                        GenericValue dummyRecord = EntityQuery.use(delegator).from("DataManagerConfig").where("configId", "DUMMY").queryOne();

                        defaultParentTx = TransactionUtil.suspend(); //Suspend T1

                        TransactionUtil.resume(runningTx); //Resume T2
                    }
                }

                TransactionUtil.commit();  //Commit T2

                TransactionUtil.resume(parentTx); //Resume T1

                if (itemInfoList.size() > 0 || isHeader)     {
                    totalItems = totalItems + itemInfoList.size();
                    dataFeedMap.put("dataFeedList", (List) itemInfoList);
                    dataFeedMap.put("isHeader", isHeader);
                    FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                    itemInfoList.clear();
                }
                writer.flush();
                writer.close();
                itemDataFeeds.put("dataFeedListSize", String.valueOf(totalItems));
                productFeatureAndAppls.close();
            } else {
                Debug.logError("Unable to generate the data feed. No Product(s) in the system have Alliance Product ID set.", module);
            }
        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation" + gee.getMessage());
        } catch (FileNotFoundException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (MalformedURLException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (IOException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (TemplateException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } finally {
            try {
                productFeatureAndAppls.close();

                try {
                    TransactionUtil.commit(); //Commit T1
                } catch (GenericTransactionException e) {
                    //log warning if error thrown when commiting transaction T1
                    Debug.logWarning("Long transaction", module);
                }                               

                if (defaultParentTx != null) TransactionUtil.resume(defaultParentTx); //Resume T0 - Default transaction
            } catch (GenericEntityException e) {
                Debug.logError("Problem closing EntityListIterator" + e.getMessage(), module);
                return ServiceUtil.returnError("Problem closing the EntityListIterator" +e.getMessage());
            }
        }
        return itemDataFeeds;
    }

    public static Map<String, Object> prepareDistributorDataFeed(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();

        Timestamp lastFeedTimestamp = (Timestamp) context.get("lastFeedTimestamp");
        List<Map<String, Object>> distributorInfoList = new LinkedList<Map<String, Object>>();
        Map<String, Object> distributorDataFeed = new HashMap<String, Object>();
        EntityListIterator productFeatureAndAppls = null;
        Map<String, Object> dataFeedMap = new HashMap<String, Object>();
        String outputDirLocation = (String) context.get("outputDirLocation");
        String fileName = (String) context.get("fileName");
        String templateLocation = (String) context.get("templateLocation");
        boolean isHeader = true;
        int totalItems =  0;
        Transaction parentTx = null;
        Transaction defaultParentTx = null;
        Transaction runningTx = null;
        Timestamp nowTimestamp = UtilDateTime.nowTimestamp();
        String includeAllProducts = EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch.ALL_PRODUCTS", delegator);

        try {
            File file = new File(outputDirLocation + "/" + fileName);
            Charset cs = Charset.forName("UTF-8");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), cs));
            EntityCondition eligibleProductCond = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_7"),
                    EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_8"),
                    EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_9"),
                    EntityCondition.makeCondition("statusId", EntityOperator.EQUALS, null)
            ), EntityOperator.OR);
            EntityCondition cond = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("productTypeId", EntityOperator.EQUALS, "FINISHED_GOOD"),
                    EntityCondition.makeCondition("domainPartyId", EntityOperator.EQUALS, "AAPA"),
                    EntityCondition.makeCondition("primaryProductCategoryId", EntityOperator.NOT_EQUAL, null)
            ), EntityOperator.AND);
            eligibleProductCond = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.AND, cond);

            if (UtilValidate.isNotEmpty(includeAllProducts) && "N".equals(includeAllProducts)) {
                EntityCondition dateCondition = EntityCondition.makeCondition(UtilMisc.toList(
                        EntityCondition.makeCondition("lastModifiedDate", EntityOperator.GREATER_THAN, lastFeedTimestamp),
                        EntityCondition.makeCondition("productUpdateDate", EntityOperator.GREATER_THAN, lastFeedTimestamp)
                ), EntityOperator.OR);
                eligibleProductCond = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.AND, dateCondition);
            }
            EntityCondition disDateCondition = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("salesDiscontinuationDate", EntityOperator.EQUALS, null),
                    EntityCondition.makeCondition("salesDiscontinuationDate", EntityOperator.GREATER_THAN_EQUAL_TO, nowTimestamp)
            ), EntityOperator.OR);
            
            
            eligibleProductCond = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.AND, disDateCondition);
            defaultParentTx = TransactionUtil.suspend();//Suspend T0 - Default Transaction
            try {
                TransactionUtil.begin();  //Begin T1
            } catch (GenericTransactionException gte) {
                Debug.logError(gte, "Unable to begin transaction T1", module);
            }


            productFeatureAndAppls = EntityQuery.use(delegator).from("HawkConfiguredProducts").where(eligibleProductCond).orderBy("aaiaBrandId").fetchSize(Integer.MIN_VALUE).queryIterator();
            
            parentTx = TransactionUtil.suspend(); //Suspend T1
            try {
                TransactionUtil.begin();  //Begin T2
            } catch (GenericTransactionException gte) {
                Debug.logError(gte, "Unable to begin transaction T2", module);
            }
            
            if (productFeatureAndAppls != null) {
                GenericValue memberProduct = null;
                List<GenericValue> legacyBrandMappings = null;
                Map<String, Object> distributorProductsInfoMap = null;
                GenericValue productFeatureAndAppl = null;
                List<EntityExpr> globalWarrantyExprs = null;

                // Prepare conditions to find products with configured WDs
                List<GenericValue> partyAttributes = EntityQuery.use(delegator).from("PartyAttribute")
                    .where("attrName", "HAWKSEARCH_ENABLED", "attrValue", "Y").queryList();
                List<String> configuredWDs = EntityUtil.getFieldListFromEntityList(partyAttributes, "partyId", true);

                EntityConditionList<EntityExpr> wdCondition = EntityCondition.makeCondition(
                    EntityCondition.makeCondition("partyIdFrom", EntityOperator.IN, configuredWDs),
                    EntityCondition.makeCondition("partyRelationshipTypeId", EntityOperator.EQUALS, "GROUP_ROLLUP"),
                    EntityCondition.makeCondition("roleTypeIdFrom", EntityOperator.EQUALS, "MEMBER"),
                    EntityCondition.makeCondition("roleTypeIdTo", EntityOperator.EQUALS, "DOMAIN_PARTY"));
                List<GenericValue> wdRelationships = EntityQuery.use(delegator).from("PartyRelationship").where(wdCondition).queryList();
                List<String> domainPartyIds = EntityUtil.getFieldListFromEntityList(wdRelationships, "partyIdTo", true);

                while ((productFeatureAndAppl = productFeatureAndAppls.next()) != null) {

                    Debug.logInfo("Part Number | ProductId [" + productFeatureAndAppl.getString("partNumber") + " | " + productFeatureAndAppl.getString("productId") + "]", module);
                    String globalProductId =  productFeatureAndAppl.getString("productId");
                    String description = productFeatureAndAppl.getString("allianceProductId");
                    if (UtilValidate.isEmpty(description)) {
                        Debug.logError("Alliance Product ID is not found for the product : " + globalProductId, module);
                        continue;
                    }

                    // If the product info is already sent in earlier feed and is not updated since the last feed time, then it can be avoided
                    if (UtilValidate.isNotEmpty(includeAllProducts) && "N".equals(includeAllProducts)) {
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("isHawkIndexed")) && "Y".equals(productFeatureAndAppl.getString("isHawkIndexed"))) {
                        if (UtilValidate.isNotEmpty(lastFeedTimestamp) && UtilValidate.isNotEmpty(productFeatureAndAppl.getTimestamp("lastModifiedDate")) && lastFeedTimestamp.after(productFeatureAndAppl.getTimestamp("lastModifiedDate"))) {
                            Debug.logError("The product info is already sent in the earlier feed : " + globalProductId, module);
                            continue;
                        }
                    }
                    } else {
                        //Debug.logInfo("Include all products set as Y", module);
                    }
                    // Get warranty information
                    String globalProductWarrantyInfo = null;
                    globalWarrantyExprs = new ArrayList<EntityExpr>();
                    globalWarrantyExprs.add(EntityCondition.makeCondition("productId", EntityOperator.EQUALS, globalProductId));
                    globalWarrantyExprs.add(EntityCondition.makeCondition("productFeatureTypeId", EntityOperator.IN, UtilMisc.toList("WARRANTY_DISTANCE", "WARRANTY_SPECIAL", "WARRANTY_TIME")));
                    List<GenericValue> globalProductWarranties = EntityQuery.use(delegator).from("ProductFeatureAndAppl").where(globalWarrantyExprs).queryList();
                    int counter = 0;
                    for (GenericValue globalProductWarranty : globalProductWarranties) {
                        counter = counter + 1;
                        globalProductWarrantyInfo = globalProductWarranty.getString("description");
                        GenericValue globalWarrantyUom = EntityQuery.use(delegator).from("Uom").where("uomId", globalProductWarranty.getString("uomId")).queryOne();
                        if (UtilValidate.isNotEmpty(globalWarrantyUom)) {
                            globalProductWarrantyInfo = globalProductWarrantyInfo + " " + globalWarrantyUom.getString("abbreviation");
                        } else {
                            globalProductWarrantyInfo = globalProductWarrantyInfo + " NA";
                        }
                        if (counter <= (globalProductWarranties.size()-1)) {
                            globalProductWarrantyInfo = globalProductWarrantyInfo + " | ";
                        }
                    }
                    // Identify all the member product(s) using brand information
                    EntityConditionList<EntityExpr> memberCondition = EntityCondition.makeCondition(
                        EntityCondition.makeCondition("aaiaBrandId", EntityOperator.EQUALS, productFeatureAndAppl.getString("aaiaBrandId")),
                        EntityCondition.makeCondition("domainPartyId", EntityOperator.IN, domainPartyIds));
                    legacyBrandMappings = EntityQuery.use(delegator).select("domainPartyId", "mfrLineCode").from("LegacyBrandMapping").where(memberCondition).orderBy("mfrLineCode").distinct().queryList();
                    for (GenericValue legacyBrandMapping: legacyBrandMappings) {
                        EntityCondition eligibleMemberProductCond = EntityCondition.makeCondition(UtilMisc.toList(
                            EntityCondition.makeCondition("stdPartNumber", EntityOperator.EQUALS, productFeatureAndAppl.getString("stdPartNumber")),
                            EntityCondition.makeCondition("domainPartyId", EntityOperator.EQUALS, legacyBrandMapping.getString("domainPartyId")),
                            EntityCondition.makeCondition("mfrLineCode", EntityOperator.EQUALS, legacyBrandMapping.getString("mfrLineCode"))
                        ), EntityOperator.AND);

                        if (UtilValidate.isNotEmpty(includeAllProducts) && "N".equals(includeAllProducts)) {
                            EntityCondition memberDateCondition = EntityCondition.makeCondition("lastModifiedDate", EntityOperator.GREATER_THAN, lastFeedTimestamp);
                            eligibleMemberProductCond = EntityCondition.makeCondition(eligibleMemberProductCond, EntityOperator.AND, memberDateCondition);
                        }
                        eligibleMemberProductCond = EntityCondition.makeCondition(eligibleMemberProductCond, EntityOperator.AND, disDateCondition);
                        memberProduct = EntityQuery.use(delegator).from("Product").where(eligibleMemberProductCond).queryFirst();
                        if (memberProduct != null) {
                            String memberProductId = memberProduct.getString("productId");
                            if (UtilValidate.isEmpty(memberProduct.getString("partNumber"))) {
                                Debug.logError("Part Number is not found for the product : " + memberProductId, module);
                                continue;
                            }
                            // Prepare the infoMap to saved for each member product
                            distributorProductsInfoMap = new HashMap<String, Object>();
                                distributorProductsInfoMap.put("uniqueId", description);
                            GenericValue lineCode = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", memberProduct.getString("mfrLineCode")).queryOne();
                            if (lineCode != null) {
                                if (UtilValidate.isNotEmpty(lineCode.getString("categoryName"))) {
                                    distributorProductsInfoMap.put("lineCode", formatContentForCsv(lineCode.getString("categoryName").toString()));
                                }
                                // Prepare the productId(SKU) for the member product using lineCode and stdPartNumber
                                String productId = lineCode.getString("categoryName") + memberProduct.getString("stdPartNumber");
                                if (UtilValidate.isNotEmpty(productId)) {
                                    distributorProductsInfoMap.put("productId", productId);
                                }
                            }
                            GenericValue sublineCode = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", memberProduct.getString("mfrSubLineCode")).queryOne();
                            if (sublineCode != null) {
                                if (UtilValidate.isNotEmpty(sublineCode.getString("categoryName"))) {
                                distributorProductsInfoMap.put("sublineCode", formatContentForCsv(sublineCode.getString("categoryName").toString()));
                                }
                            }
                            String domainPartyId = memberProduct.getString("domainPartyId");
                            GenericValue domainParty = EntityQuery.use(delegator).from("PartyGroup").where("partyId", domainPartyId).queryOne();
                            if (domainParty != null) {
                                distributorProductsInfoMap.put("distributorId", domainParty.getString("groupName"));
                            }
                            GenericValue jobberPrice = EntityQuery.use(delegator).from("ProductPrice").where("productId", memberProductId, "productPriceTypeId", "JOBBER_PRICE").filterByDate().queryFirst();
                            // Member Products storefront configuration
                            List<GenericValue> productStoreCatalogs = EntityQuery.use(delegator).from("ProductStoreCatalog").where(EntityCondition.makeCondition("prodCatalogId", EntityOperator.NOT_EQUAL, null)).queryList();
                            if (productStoreCatalogs != null) {
                                int storeCount = 0;
                                String saleConfig = "";
                                for (GenericValue productStoreCatalog : productStoreCatalogs) {
                                    GenericValue saleableProdCatalogDetail = EntityQuery.use(delegator).from("ProductCategoryMemberAndCatalogCategory").where("productId", memberProductId, "prodCatalogCategoryTypeId", "PCCT_PURCH_ALLW", "prodCatalogId", productStoreCatalog.getString("prodCatalogId")).filterByDate().queryFirst();
                                    GenericValue productStore = EntityQuery.use(delegator).from("ProductStore").where("productStoreId", productStoreCatalog.getString("productStoreId")).queryFirst();
                                    if (saleableProdCatalogDetail != null) {
                                        if (UtilValidate.isNotEmpty(productStore.getString("storeName"))) {
                                            saleConfig = productStore.getString("storeName");
                                        } else {
                                            saleConfig = productStore.getString("productStoreId");
                                        }
                                    }
                                    if (counter <= (productStoreCatalogs.size()-1)) {
                                        saleConfig = saleConfig + " | ";
                                    }
                                    storeCount = storeCount + 1;
                                }
                            }
                            if (jobberPrice != null) {
                                distributorProductsInfoMap.put("priceRetail", jobberPrice.getBigDecimal("price"));
                            }
                            GenericValue wholeSalePrice = EntityQuery.use(delegator).from("ProductPrice").where("productId", memberProductId, "productPriceTypeId", "WHOLESALE_PRICE").filterByDate().queryFirst();
                            if (wholeSalePrice != null){
                                distributorProductsInfoMap.put("priceSale", wholeSalePrice.getBigDecimal("price"));
                            }
                            GenericValue corePrice = EntityQuery.use(delegator).from("ProductPrice").where("productId", memberProductId, "productPriceTypeId", "CORE_PRICE").filterByDate().queryFirst();
                            if (corePrice != null){
                                distributorProductsInfoMap.put("priceSpecial", corePrice.getBigDecimal("price"));
                                distributorProductsInfoMap.put("priceCore", corePrice.getBigDecimal("price"));
                            }
                            GenericValue specialPromoPrice = EntityQuery.use(delegator).from("ProductPrice").where("productId", memberProductId, "productPriceTypeId", "SPECIAL_PROMO_PRICE").filterByDate().queryFirst();
                            if (specialPromoPrice != null){
                                distributorProductsInfoMap.put("priceSale", specialPromoPrice.getBigDecimal("price"));
                            }
                            if (UtilValidate.isEmpty(productFeatureAndAppl.getString("nationalPopularityCode"))) {
                                Debug.logWarning("National Popularity Code is not found for the product : " + memberProductId, module);
                            }
                            distributorProductsInfoMap.put("sortDefault", EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch." + productFeatureAndAppl.getString("nationalPopularityCode"), "70000000", delegator));
                            if (UtilValidate.isNotEmpty(globalProductWarrantyInfo)) {
                                distributorProductsInfoMap.put("warranty", formatContentForCsv(globalProductWarrantyInfo.toString()));
                            }
                            distributorInfoList.add(distributorProductsInfoMap);

                            if (distributorInfoList.size() % 1000 == 0)     {
                                totalItems = totalItems + distributorInfoList.size();
                                dataFeedMap.put("dataFeedList", (List) distributorInfoList);
                                dataFeedMap.put("isHeader", isHeader);
                                FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                                distributorInfoList.clear();
                                isHeader = false;

                                TransactionUtil.commit(); //Commit T2
                                
                                runningTx = TransactionUtil.suspend(); //Suspend T2

                                TransactionUtil.resume(defaultParentTx); //Resume T1

                                GenericValue dummyRecord = EntityQuery.use(delegator).from("DataManagerConfig").where("configId", "DUMMY").queryOne();

                                defaultParentTx = TransactionUtil.suspend(); //Suspend T1

                                TransactionUtil.resume(runningTx); //Resume T2

                            }
                        } else {
                            /*Debug.logWarning("No Product exists in the system with partNumber [" + productFeatureAndAppl.getString("partNumber")
                                    + "], Domain Party [" + legacyBrandMapping.getString("domainPartyId") + "], Line Code ["
                                    + legacyBrandMapping.getString("mfrLineCode") + "] and Sub Line Code ["
                                    + legacyBrandMapping.getString("mfrSubLineCode") + "].", module);*/
                        }
                    }
                }
                TransactionUtil.commit();  //Commit T2

                TransactionUtil.resume(parentTx); //Resume T1

                if (distributorInfoList.size() > 0 || isHeader)     {
                    totalItems = totalItems + distributorInfoList.size();
                    dataFeedMap.put("dataFeedList", (List) distributorInfoList);
                    dataFeedMap.put("isHeader", isHeader);
                    FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                    distributorInfoList.clear();
                }
                writer.flush();
                writer.close();
                distributorDataFeed.put("dataFeedListSize", String.valueOf(totalItems));
            } else {
                Debug.logError("Unable to generate the data feed. No Product(s) in the system have Alliance Product ID set.", module);
            }

        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation ");
        } catch (FileNotFoundException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (MalformedURLException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (IOException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (TemplateException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } finally {
            try {
                productFeatureAndAppls.close();

                try {
                    TransactionUtil.commit(); //Commit T1
                } catch (GenericTransactionException e) {
                    //log warning if error thrown when commiting transaction T1
                    Debug.logWarning("Long transaction", module);
                }

                if (defaultParentTx != null) TransactionUtil.resume(defaultParentTx); //Resume T0 - Default transaction
            } catch (GenericEntityException e) {
                Debug.logError("Problem closing EntityListIterator" + e.getMessage(), module);
                return ServiceUtil.returnError("Problem closing EntityListIterator" +e.getMessage());
            }
        }
        return distributorDataFeed;
    }

    // Export PCDB hierarchy to a TEXT file.
    public static Map<String, Object> prepareHierarchyDataFeed(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();

        List<Map<String, Object>> categoryHierarchy = new ArrayList<Map<String, Object>>();
        Map<String, Object> categoryDataFeed = new HashMap<String, Object>();
        EntityListIterator productCategories = null;

        Map<String, Object> dataFeedMap = new HashMap<String, Object>();
        String outputDirLocation = (String) context.get("outputDirLocation");
        String fileName = (String) context.get("fileName");
        String templateLocation = (String) context.get("templateLocation");
        boolean isHeader = true;
        int totalItems =  0;

        try {
            File file = new File(outputDirLocation + "/" + fileName);
            Charset cs = Charset.forName("UTF-8");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), cs));
            // Prepare category data
            EntityConditionList<EntityExpr> categoryCondition = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("productCategoryTypeId", EntityOperator.EQUALS, "AAIA_CATEGORY"),
                    EntityCondition.makeCondition("productCategoryTypeId", EntityOperator.EQUALS, "AAIA_SUBCATEGORY"),
                    EntityCondition.makeCondition("productCategoryTypeId", EntityOperator.EQUALS, "AAIA_TERMINOLOGY")), EntityOperator.OR);
            productCategories = EntityQuery.use(delegator).from("ProductCategoryAndCategory").where(categoryCondition).queryIterator();
            if (UtilValidate.isNotEmpty(productCategories)) {
                GenericValue productCategory = null;
                while ((productCategory = productCategories.next()) != null) {
                    // Prepare category hierarchies
                    Map<String, Object> categoryInfoMap = new HashMap<String, Object>();
                    if (UtilValidate.isNotEmpty(productCategory.getString("primaryParentCategoryId"))) {
                        if ("AAIA_SUBCATEGORY".equals(productCategory.getString("productCategoryTypeId"))) {
                            categoryInfoMap.put("primaryParentCategoryId", String.format("%03d", Integer.parseInt(productCategory.getString("primaryParentCategoryName"))));
                        } else if ("AAIA_TERMINOLOGY".equals(productCategory.getString("productCategoryTypeId"))) {
                            String subCategoryId = String.format("%03d", Integer.parseInt(productCategory.getString("primaryParentCategoryName")));
                            GenericValue productCategoryAndCategory = EntityQuery.use(delegator).from("ProductCategoryAndCategory").where("productCategoryId", productCategory.getString("primaryParentCategoryId")).queryOne();
                            if (productCategoryAndCategory != null && UtilValidate.isNotEmpty(productCategoryAndCategory.getString("primaryParentCategoryName"))) {
                                categoryInfoMap.put("primaryParentCategoryId", String.format("%03d", Integer.parseInt(productCategoryAndCategory.getString("primaryParentCategoryName"))) + subCategoryId);
                            }
                        }
                    } else {
                        if ("AAIA_CATEGORY".equals(productCategory.getString("productCategoryTypeId"))) {
                            categoryInfoMap.put("primaryParentCategoryId", "0");
                        } else if ("AAIA_SUBCATEGORY".equals(productCategory.getString("productCategoryTypeId"))) {
                            Debug.logWarning("No parent category found for the SUBCATEGORY : " + productCategory.getString("categoryName"), module);
                            continue;
                        } else if ("AAIA_TERMINOLOGY".equals(productCategory.getString("productCategoryTypeId"))) {
                            Debug.logWarning("No parent category found for the TERMINOLOGY : " + productCategory.getString("categoryName"), module);
                            continue;
                        }
                    }

                    if ("AAIA_CATEGORY".equals(productCategory.getString("productCategoryTypeId"))) {
                        String categoryId = String.format("%03d", Integer.parseInt(productCategory.getString("categoryName")));
                        categoryInfoMap.put("productCategoryId", categoryId);
                    } else if ("AAIA_SUBCATEGORY".equals(productCategory.getString("productCategoryTypeId"))) {
                        if (UtilValidate.isNotEmpty(productCategory.getString("primaryParentCategoryName"))) {
                            String categoryId = String.format("%03d", Integer.parseInt(productCategory.getString("primaryParentCategoryName")));
                            String subCategory = String.format("%03d", Integer.parseInt(productCategory.getString("primaryParentCategoryName"))) + String.format("%03d", Integer.parseInt(productCategory.getString("categoryName")));
                            categoryInfoMap.put("productCategoryId", subCategory);
                        } else {
                            Debug.logWarning("No parent category found for the SUBCATEGORY : " + productCategory.getString("categoryName"), module);
                            continue;
                        }
                    } else if ("AAIA_TERMINOLOGY".equals(productCategory.getString("productCategoryTypeId"))) {
                        if (UtilValidate.isNotEmpty(productCategory.getString("primaryParentCategoryName"))) {
                            String terminology = String.format("%03d", Integer.parseInt(productCategory.getString("primaryParentCategoryName"))) + String.format("%05d", Integer.parseInt(productCategory.getString("categoryName")));
                            GenericValue productCategoryAndCategory = EntityQuery.use(delegator).from("ProductCategoryAndCategory").where("productCategoryId", productCategory.getString("primaryParentCategoryId")).queryOne();
                            if (productCategoryAndCategory != null && UtilValidate.isNotEmpty(productCategoryAndCategory.getString("primaryParentCategoryName"))) {
                                terminology = String.format("%03d", Integer.parseInt(productCategoryAndCategory.getString("primaryParentCategoryName"))) + terminology;
                                categoryInfoMap.put("productCategoryId", terminology);
                            } else {
                                Debug.logWarning("No category found for the TERMINOLOGY : " + productCategory.getString("categoryName"), module);
                                continue;
                            }
                        } else {
                            Debug.logWarning("No parent sub category found for the TERMINOLOGY : " + productCategory.getString("categoryName"), module);
                            continue;
                        }
                    }
                    if (UtilValidate.isNotEmpty(productCategory.getString("description"))) {
                    categoryInfoMap.put("categoryName", formatContentForCsv(productCategory.getString("description").toString()));
                    }
                    // Get the category image info
                    GenericValue categoryContent = EntityQuery.use(delegator).from("ProductCategoryContent")
                            .where("productCategoryId", productCategory.getString("productCategoryId"), "prodCatContentTypeId", "CATEGORY_IMAGE_URL")
                            .filterByDate().queryFirst();
                    if (categoryContent != null) {
                        if (UtilValidate.isNotEmpty(categoryContent.getString("contentId"))) {
                            GenericValue imageContent = EntityQuery.use(delegator).from("Content").where("contentId", categoryContent.getString("contentId")).queryFirst();
                            if (imageContent != null) {
                                GenericValue imageDataResource = EntityQuery.use(delegator).from("DataResource").where("dataResourceId", imageContent.getString("dataResourceId")).queryFirst();
                                if (imageDataResource != null) {
                                    categoryInfoMap.put("categoryImage", imageDataResource.getString("objectInfo"));
                                }
                            }
                        }
                    } else {
                        Debug.logWarning("Image is not available for the Category : " + productCategory.getString("categoryName"), module);
                    }
                    categoryHierarchy.add(categoryInfoMap);

                    if (categoryHierarchy.size() % 10000 == 0)     {
                        totalItems = totalItems + categoryHierarchy.size();
                        dataFeedMap.put("dataFeedList", (List) categoryHierarchy);
                        dataFeedMap.put("isHeader", isHeader);
                        FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                        categoryHierarchy.clear();
                        isHeader = false;
                    }
                }
            } else {
                Debug.logError("Unable to generate the hierarchy feed. No Category, Subcategory and Terminology available.", module);
            }
            if (categoryHierarchy.size() > 0)     {
                totalItems = totalItems + categoryHierarchy.size();
                dataFeedMap.put("dataFeedList", (List) categoryHierarchy);
                dataFeedMap.put("isHeader", isHeader);
                FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                categoryHierarchy.clear();
            }
            writer.flush();
            writer.close();
            categoryDataFeed.put("dataFeedListSize", String.valueOf(totalItems));
            productCategories.close();

            Debug.logInfo("Total Product Categories generated : " + productCategories.getResultsSizeAfterPartialList(), module);
        } catch (GenericEntityException e) {
            Debug.logError("Error occurred during entity operation :"+ e.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation"+ e.getMessage());
        } catch (FileNotFoundException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (MalformedURLException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (IOException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (TemplateException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } finally {
            try {
                productCategories.close();
            } catch (GenericEntityException e) {
                Debug.logError("Problem closing EntityListIterator"+ e.getMessage(), module);
                return ServiceUtil.returnError("Problem closing EntityListIterator"+ e.getMessage());
            }
        }
        return categoryDataFeed;
    }

    // Export Brands hierarchy to a TEXT file.
    public static Map<String, Object> prepareBrandHierarchyDataFeed(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();

        List<Map<String, Object>> brandHierarchy = new ArrayList<Map<String, Object>>();
        Map<String, Object> brandDataFeed = new HashMap<String, Object>();
        EntityListIterator productCategories = null;

        Map<String, Object> dataFeedMap = new HashMap<String, Object>();
        String outputDirLocation = (String) context.get("outputDirLocation");
        String fileName = (String) context.get("fileName");
        String templateLocation = (String) context.get("templateLocation");
        boolean isHeader = true;
        int totalItems =  0;

        try {
            File file = new File(outputDirLocation + "/" + fileName);
            Charset cs = Charset.forName("UTF-8");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), cs));
            // Prepare brands(ProductCategory) data
            EntityConditionList<EntityExpr> categoryCondition = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("productCategoryTypeId", EntityOperator.EQUALS, "AAIA_BRAND"),
                    EntityCondition.makeCondition("productCategoryTypeId", EntityOperator.EQUALS, "AAIA_SUBBRAND")), EntityOperator.OR);
            productCategories = EntityQuery.use(delegator).from("ProductCategoryAndCategory").where(categoryCondition).queryIterator();
            if (productCategories != null) {
                GenericValue productCategory = null;
                while ((productCategory = productCategories.next()) != null) {
                    // Prepare brand hierarchy
                    Map<String, Object> brandInfoMap = new HashMap<String, Object>();
                    if (UtilValidate.isNotEmpty(productCategory.getString("primaryParentCategoryId"))) {
                        brandInfoMap.put("parentBrandId", productCategory.getString("primaryParentCategoryName"));
                    } else {
                        if ("AAIA_BRAND".equals(productCategory.getString("productCategoryTypeId"))) {
                            brandInfoMap.put("parentBrandId", "0");
                        } else if ("AAIA_SUBBRAND".equals(productCategory.getString("productCategoryTypeId"))) {
                            Debug.logWarning("No parent brand found for the Subbrand : " + productCategory.getString("categoryName"), module);
                            continue;
                        }
                    }
                    brandInfoMap.put("brandId", productCategory.getString("categoryName"));
                    if (UtilValidate.isNotEmpty(productCategory.getString("description"))) {
                    brandInfoMap.put("brandName", formatContentForCsv(productCategory.getString("description").toString()));
                    }
                    // Get the brand image info
                    GenericValue categoryContent = EntityQuery.use(delegator).from("ProductCategoryContent")
                            .where("productCategoryId", productCategory.getString("productCategoryId"), "prodCatContentTypeId", "CATEGORY_IMAGE_URL")
                            .filterByDate().queryFirst();
                    if (categoryContent != null) {
                        if (UtilValidate.isNotEmpty(categoryContent.getString("contentId"))) {
                            GenericValue imageContent = EntityQuery.use(delegator).from("Content").where("contentId", categoryContent.getString("contentId")).queryFirst();
                            if (imageContent != null) {
                                GenericValue imageDataResource = EntityQuery.use(delegator).from("DataResource").where("dataResourceId", imageContent.getString("dataResourceId")).queryFirst();
                                if (imageDataResource != null) {
                                    brandInfoMap.put("brandImage", imageDataResource.getString("objectInfo"));
                                }
                            }
                        }
                    } else {
                        Debug.logWarning("Image is not available for the Category : " + productCategory.getString("categoryName"), module);
                    }
                    brandHierarchy.add(brandInfoMap);

                    if (brandHierarchy.size() % 10000 == 0)     {
                        totalItems = totalItems + brandHierarchy.size();
                        dataFeedMap.put("dataFeedList", (List) brandHierarchy);
                        dataFeedMap.put("isHeader", isHeader);
                        FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                        brandHierarchy.clear();
                        isHeader = false;
                    }
                }
            } else {
                Debug.logError("Unable to generate the brand hierarchy feed. No Brands and Subbrands available.", module);
            }
            if (brandHierarchy.size() > 0)     {
                totalItems = totalItems + brandHierarchy.size();
                dataFeedMap.put("dataFeedList", (List) brandHierarchy);
                dataFeedMap.put("isHeader", isHeader);
                FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                brandHierarchy.clear();
            }
            writer.flush();
            writer.close();
            brandDataFeed.put("dataFeedListSize", String.valueOf(totalItems));
            productCategories.close();

            Debug.logInfo("Total Brands/Subbrands exported : " + productCategories.getResultsSizeAfterPartialList(), module);
        } catch (GenericEntityException e) {
            Debug.logError("Error occurred during entity operation :"+ e.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation"+ e.getMessage());
        } catch (FileNotFoundException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (MalformedURLException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (IOException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (TemplateException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } finally {
            try {
                productCategories.close();
            } catch (GenericEntityException e) {
                Debug.logError("Problem closing EntityListIterator"+ e.getMessage(), module);
                return ServiceUtil.returnError("Problem closing EntityListIterator"+ e.getMessage());
            }
        }

        return brandDataFeed;
    }

    public static Map<String, Object> prepareAttributesDataFeed (DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();
        Locale locale = (Locale) context.get("locale");

        Timestamp lastFeedTimestamp = (Timestamp) context.get("lastFeedTimestamp");
        List<Map<String, Object>> attributeInfoList = new LinkedList<Map<String, Object>>();
        Map<String, Object> attributesDataFeed = new HashMap<String, Object>();
        EntityListIterator productFeatureAndAppls = null;
        Map<String, Object> dataFeedMap = new HashMap<String, Object>();
        String outputDirLocation = (String) context.get("outputDirLocation");
        String fileName = (String) context.get("fileName");
        String templateLocation = (String) context.get("templateLocation");
        String includeProducts = EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch.ALL_PRODUCTS", delegator);
        Timestamp nowTimestamp = UtilDateTime.nowTimestamp();
        Transaction parentTx = null;
        Transaction defaultParentTx = null;
        Transaction runningTx = null;
        boolean isHeader = true;
        int totalItems =  0;

        try {
            File file = new File(outputDirLocation + "/" + fileName);
            Charset cs = Charset.forName("UTF-8");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), cs));
            EntityCondition eligibleProductCond = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_7"),
                    EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_8"),
                    EntityCondition.makeCondition("statusId", EntityOperator.NOT_EQUAL, "PRODUCT_STS_9"),
                    EntityCondition.makeCondition("statusId", EntityOperator.EQUALS, null)
            ), EntityOperator.OR);
            EntityCondition cond = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("productTypeId", EntityOperator.EQUALS, "FINISHED_GOOD"),
                    EntityCondition.makeCondition("domainPartyId", EntityOperator.EQUALS, "AAPA"),
                    EntityCondition.makeCondition("primaryProductCategoryId", EntityOperator.NOT_EQUAL, null)
            ), EntityOperator.AND);
            eligibleProductCond = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.AND, cond);

            if (UtilValidate.isNotEmpty(includeProducts) && "N".equals(includeProducts)) {
                EntityCondition dateCondition = EntityCondition.makeCondition(UtilMisc.toList(
                        EntityCondition.makeCondition("lastModifiedDate", EntityOperator.GREATER_THAN, lastFeedTimestamp),
                        EntityCondition.makeCondition("productUpdateDate", EntityOperator.GREATER_THAN, lastFeedTimestamp)
                ), EntityOperator.OR);
                eligibleProductCond = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.AND, dateCondition);
            }
            EntityCondition disDateCondition = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("salesDiscontinuationDate", EntityOperator.EQUALS, null),
                    EntityCondition.makeCondition("salesDiscontinuationDate", EntityOperator.GREATER_THAN_EQUAL_TO, nowTimestamp)
            ), EntityOperator.OR);
            eligibleProductCond = EntityCondition.makeCondition(eligibleProductCond, EntityOperator.AND, disDateCondition);
            defaultParentTx = TransactionUtil.suspend();//Suspend T0 - Default Transaction
            try {
                TransactionUtil.begin();  //Begin T1
            } catch (GenericTransactionException gte) {
                Debug.logError(gte, "Unable to begin transaction T1", module);
            }
            productFeatureAndAppls = EntityQuery.use(delegator).from("HawkConfiguredProducts").where(eligibleProductCond).orderBy("aaiaBrandId").fetchSize(Integer.MIN_VALUE).queryIterator();
            parentTx = TransactionUtil.suspend(); //Suspend T1
            try {
                TransactionUtil.begin();  //Begin T2
            } catch (GenericTransactionException gte) {
                Debug.logError(gte, "Unable to begin transaction T2", module);
            }
            if (productFeatureAndAppls != null) {
                GenericValue productFeatureAndAppl = null;
                List<EntityExpr> globalWarrantyExprs = null;

                String includeAllProducts = EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch.ALL_PRODUCTS", delegator);
                String globalProductId = null;
                String uniqueId = null;
                GenericValue globalBrand = null;
                GenericValue globalSubBrand = null;
                GenericValue brandOwnerRole = null;
                GenericValue parentCompanyRole = null;
                GenericValue terminologyRollup = null;
                String globalProductWarrantyInfo;
                List<EntityExpr> gtinItemLevelExpr;
                GenericValue gtinIdentification = null;
                GenericValue vmrsIdentification = null;
                GenericValue productAttrCT = null;
                List<GenericValue> productFeatureAttr;
                List<GenericValue> globalProductWarranties;
                while ((productFeatureAndAppl = productFeatureAndAppls.next()) != null) {
                    Debug.logInfo("Part Number | ProductId [" + productFeatureAndAppl.getString("partNumber") + " | " + productFeatureAndAppl.getString("productId") + "]", module);
                    globalProductId =  productFeatureAndAppl.getString("productId");
                    uniqueId = productFeatureAndAppl.getString("allianceProductId");
                    if (UtilValidate.isEmpty(uniqueId)) {
                        Debug.logError("Alliance Product ID is not found for the product : " + globalProductId, module);
                        continue;
                    }

                    if (UtilValidate.isEmpty(productFeatureAndAppl.getString("partNumber"))) {
                        Debug.logError("Part Number is not found for the product : " + globalProductId, module);
                        continue;
                    }

                    // If the product info is already sent in earlier feed and is not updated since the last feed time, then it can be avoided
                    if (UtilValidate.isNotEmpty(includeAllProducts) && "N".equals(includeAllProducts)) {
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("isHawkIndexed")) && "Y".equals(productFeatureAndAppl.getString("isHawkIndexed"))) {
                        if (UtilValidate.isNotEmpty(lastFeedTimestamp) && UtilValidate.isNotEmpty(productFeatureAndAppl.getTimestamp("lastModifiedDate")) && lastFeedTimestamp.after(productFeatureAndAppl.getTimestamp("lastModifiedDate"))) {
                            Debug.logError("The product info is already sent in the earlier feed : " + globalProductId, module);
                            continue;
                        }
                    }
                    }

                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("partNumber"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "part-number", "value", productFeatureAndAppl.getString("partNumber")));
                    }
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("stdPartNumber"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "part-key", "value", productFeatureAndAppl.getString("stdPartNumber")));
                    }

                    // currently productType is set to 'aa'
                    attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "productType", "value", "aa"));

                    // Global Products storefront configuration
                    List<GenericValue> productStoreCatalogs = EntityQuery.use(delegator).from("ProductStoreCatalog").where(EntityCondition.makeCondition("prodCatalogId", EntityOperator.NOT_EQUAL, null)).queryList();
                    List<String> saleConfigs = new ArrayList<String> ();
                    if (productStoreCatalogs != null) {
                        for (GenericValue productStoreCatalog : productStoreCatalogs) {
                            GenericValue saleableProdCatalogDetail = EntityQuery.use(delegator).from("ProductCategoryMemberAndCatalogCategory").where("productId", globalProductId, "prodCatalogCategoryTypeId", "PCCT_PURCH_ALLW", "prodCatalogId", productStoreCatalog.getString("prodCatalogId")).filterByDate().queryFirst();
                            String saleConfig = "";
                            GenericValue productStore = EntityQuery.use(delegator).from("ProductStore").where("productStoreId", productStoreCatalog.getString("productStoreId")).queryFirst();
                            if (saleableProdCatalogDetail != null) {
                                if (UtilValidate.isNotEmpty(productStore.getString("storeName"))) {
                                    attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "storefront", "value", formatContentForCsv(productStore.getString("storeName"))));
                                } else {
                                    attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "storefront", "value", formatContentForCsv(productStore.getString("productStoreId"))));
                                }
                            }
                        }
                    }
                    
                    // Brand Info
                    globalBrand = productFeatureAndAppl.getRelatedOne("AaiaBrandCategoryProductCategory", false);
                    if (globalBrand != null) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "brand-id", "value", globalBrand.getString("categoryName")));
                        if (UtilValidate.isNotEmpty(globalBrand.getString("description"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "brand", "value", formatContentForCsv(globalBrand.getString("description").toString())));
                        }
                    } else {
                        Debug.logError("Brand is not available for the product : " + globalProductId, module);
                        continue;
                    }

                    // Sub Brand Info
                    globalSubBrand = productFeatureAndAppl.getRelatedOne("AaiaSubBrandCategoryProductCategory", false);
                    if (globalSubBrand != null) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "subbrand-id", "value", globalSubBrand.getString("categoryName")));
                        if (UtilValidate.isNotEmpty(globalSubBrand.getString("description"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "subbrand", "value", formatContentForCsv(globalSubBrand.getString("description").toString())));
                        }
                    }

                    // Brand Owner Info
                    brandOwnerRole = EntityQuery.use(delegator).from("ProductCategoryRole").where("productCategoryId", productFeatureAndAppl.getString("aaiaBrandId"), "roleTypeId", "MANUFACTURER").filterByDate().queryFirst();
                    if (brandOwnerRole != null) {
                        GenericValue brandSupplierParty = brandOwnerRole.getRelatedOne("Party", false);
                        GenericValue brandSupplierGroup = brandSupplierParty.getRelatedOne("PartyGroup", false);
                        if (brandSupplierGroup != null) {
                            if (UtilValidate.isNotEmpty(brandSupplierGroup.getString("groupName"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "brand-owner", "value", formatContentForCsv(brandSupplierGroup.getString("groupName").toString())));
                            }
                        }
                    }

                    // Parent Company info
                    parentCompanyRole = EntityQuery.use(delegator).from("ProductCategoryRole").where("productCategoryId", productFeatureAndAppl.getString("aaiaBrandId"), "roleTypeId", "PARENT_ORGANIZATION").filterByDate().queryFirst();
                    if (parentCompanyRole != null) {
                        GenericValue parentCompanyParty = parentCompanyRole.getRelatedOne("Party", false);
                        GenericValue parentCompanyGroup = parentCompanyParty.getRelatedOne("PartyGroup", false);
                        if (parentCompanyGroup != null) {
                            if (UtilValidate.isNotEmpty(parentCompanyGroup.getString("groupName"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "parent-company", "value", formatContentForCsv(parentCompanyGroup.getString("groupName").toString())));
                            }
                        }
                    }

                    // product category and its hierarchy
                    terminologyRollup = EntityQuery.use(delegator).from("TerminologyHierarchy")
                            .where("productCategoryId", productFeatureAndAppl.getString("primaryProductCategoryId")).queryOne();
                    if (terminologyRollup != null) {
                        if (UtilValidate.isNotEmpty(terminologyRollup.getString("categoryName"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "terminology-ID", "value", terminologyRollup.getString("categoryName")));
                            if (UtilValidate.isNotEmpty(terminologyRollup.getString("description"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "terminology", "value", formatContentForCsv(terminologyRollup.getString("description").toString())));
                            }
                            String categoryId = "";
                            if (UtilValidate.isNotEmpty(terminologyRollup.getString("categoryName"))) {
                                categoryId = String.format("%05d", Integer.parseInt(terminologyRollup.getString("categoryName")));
                                if (UtilValidate.isNotEmpty(terminologyRollup.getString("parentCategoryName"))) {
                                    categoryId = String.format("%03d", Integer.parseInt(terminologyRollup.getString("parentCategoryName"))) + categoryId;
                                    if (UtilValidate.isNotEmpty(terminologyRollup.getString("topCategoryName"))) {
                                        categoryId = String.format("%03d", Integer.parseInt(terminologyRollup.getString("topCategoryName"))) + categoryId;
                                    }
                                }
                            }
                            if (UtilValidate.isNotEmpty(categoryId)) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "category_id", "value", categoryId));
                            }
                        }
                        if (UtilValidate.isNotEmpty(terminologyRollup.getString("description"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "category", "value", formatContentForCsv(terminologyRollup.getString("description").toString())));
                        }
                        if (UtilValidate.isNotEmpty(terminologyRollup.getString("parentCategoryName"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "ac-h1-ID", "value", terminologyRollup.getString("topCategoryName")));
                        }
                        if (UtilValidate.isNotEmpty(terminologyRollup.getString("parentCategoryDesc"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "ac-h1", "value", terminologyRollup.getString("topCategoryDesc")));
                        }
                        if (UtilValidate.isNotEmpty(terminologyRollup.getString("topCategoryName"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "ac-h2-ID", "value", terminologyRollup.getString("parentCategoryName")));
                        }
                        if (UtilValidate.isNotEmpty(terminologyRollup.getString("topCategoryDesc"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "ac-h2", "value", terminologyRollup.getString("parentCategoryDesc")));
                        }
                    }

                    // Get warranty information
                    globalProductWarrantyInfo = null;
                    globalWarrantyExprs = new ArrayList<EntityExpr>();
                    globalWarrantyExprs.add(EntityCondition.makeCondition("productId", EntityOperator.EQUALS, globalProductId));
                    globalWarrantyExprs.add(EntityCondition.makeCondition("productFeatureTypeId", EntityOperator.IN, UtilMisc.toList("WARRANTY_DISTANCE", "WARRANTY_SPECIAL", "WARRANTY_TIME")));
                    globalProductWarranties = EntityQuery.use(delegator).from("ProductFeatureAndAppl").where(globalWarrantyExprs).queryList();
                    int counter = 0;
                    for (GenericValue globalProductWarranty : globalProductWarranties) {
                        counter = counter + 1;
                        globalProductWarrantyInfo = globalProductWarranty.getString("description");
                        GenericValue globalWarrantyUom = EntityQuery.use(delegator).from("Uom").where("uomId", globalProductWarranty.getString("uomId")).queryOne();
                        if (UtilValidate.isNotEmpty(globalWarrantyUom)) {
                            globalProductWarrantyInfo = globalProductWarrantyInfo + " " + globalWarrantyUom.getString("abbreviation");
                        } else {
                            globalProductWarrantyInfo = globalProductWarrantyInfo + " NA";
                        }
                        if (counter <= (globalProductWarranties.size()-1)) {
                            globalProductWarrantyInfo = globalProductWarrantyInfo + " | ";
                        }
                    }
                    if (UtilValidate.isNotEmpty(globalProductWarrantyInfo)) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "warranty", "value", formatContentForCsv(globalProductWarrantyInfo.toString())));
                    }

                    // PIES B10 and B11
                    gtinItemLevelExpr = new ArrayList<EntityExpr>();
                    gtinItemLevelExpr.add(EntityCondition.makeCondition("productId", EntityOperator.EQUALS, globalProductId));
                    gtinItemLevelExpr.add(EntityCondition.makeCondition("goodIdentificationTypeId", EntityOperator.IN, UtilMisc.toList("UPCA", "EAN")));
                    gtinIdentification = EntityQuery.use(delegator).from("GoodIdentification").where(gtinItemLevelExpr).queryFirst();
                    if (gtinIdentification != null) {
                        String gtinQualifier = null;
                        if ("EAN".equals(gtinIdentification.getString("goodIdentificationTypeId"))) {
                            gtinQualifier = "EA";
                        } else {
                            gtinQualifier = "UP";
                        }
                        if (UtilValidate.isNotEmpty(gtinQualifier)) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Item-Level GTIN Qualifier", "value", gtinQualifier));
                        }
                        if (UtilValidate.isNotEmpty(gtinIdentification.getString("idValue"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Item-Level GTIN", "value", gtinIdentification.getString("idValue")));
                        }
                    }


                    // B65
                    vmrsIdentification = EntityQuery.use(delegator).from("GoodIdentification").where("productId", globalProductId, "goodIdentificationTypeId", "VMRS").queryOne();
                    if (vmrsIdentification != null) {
                        if (UtilValidate.isNotEmpty(vmrsIdentification.getString("idValue"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "VMRS Code", "value", vmrsIdentification.getString("idValue")));
                        }
                    }

                    // B32
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getBigDecimal("fixedAmount"))) {
                    attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Item Quantity Size Description", "value", productFeatureAndAppl.getBigDecimal("fixedAmount")));
                    }

                    // B32
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("quantityUomId"))) {
                    attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Item Quantity Size UOM Description", "value", productFeatureAndAppl.getString("quantityUomId")));
                    }

                    //B34
                    productAttrCT = EntityQuery.use(delegator).from("ProductAttribute").where("productId", globalProductId, "attrName", "CONTAINER_TYPE").queryOne();
                    if (productAttrCT != null) {
                        if (UtilValidate.isNotEmpty(productAttrCT.getString("attrValue"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Container Type Description", "value", formatContentForCsv(productAttrCT.getString("attrValue").toString())));
                        }
                    }

                    // C05(ASM, DES, EXT, INV, KEY, MKT, SHO)
                    ProductContentWrapper productContentWrapper = new ProductContentWrapper(dispatcher, productFeatureAndAppl.getRelatedOne("Product", false), locale, "text/plain");
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("APP_SUMMARY", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Application Summary", "value", formatContentForCsv(productContentWrapper.get("APP_SUMMARY", "string").toString())));
                    }
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("LONG_DESCRIPTION", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Product Description - Long - 80", "value", formatContentForCsv(productContentWrapper.get("LONG_DESCRIPTION", "string").toString())));
                    }
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("EXTENDED_DESC", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Product Description - Extended 240", "value", formatContentForCsv(productContentWrapper.get("EXTENDED_DESC", "string").toString())));
                    }
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("INVOICE_DESC", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Product Description - Invoice - 40", "value", formatContentForCsv(productContentWrapper.get("INVOICE_DESC", "string").toString())));
                    }
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("KEYWORD_SEARCH", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Key Search Words - 2000", "value", formatContentForCsv(productContentWrapper.get("KEYWORD_SEARCH", "string").toString())));
                    }
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("MARKETING_DESC", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Marketing Description - 2000", "value", formatContentForCsv(productContentWrapper.get("MARKETING_DESC", "string").toString())));
                    }
                    if (UtilValidate.isNotEmpty(productContentWrapper.get("DESCRIPTION", "string").toString())) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Product Description - Short - 20", "value", formatContentForCsv(productContentWrapper.get("DESCRIPTION", "string").toString())));
                    }

                    // Attributes (Custom and PADb)
                    productFeatureAttr = EntityQuery.use(delegator).from("ProductFeatureAttributes").where("productId", globalProductId).filterByDate().queryList();
                    for (GenericValue productFeature : productFeatureAttr) {
                        if ("aca_eCatType".equals(productFeature.getString("featureTypeDesc"))) {
                            if (UtilValidate.isNotEmpty(productFeature.getString("featureDescription"))) {
                            attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "catalog-manager", "value", formatContentForCsv(productFeature.getString("featureDescription").toString())));
                            }
                            continue;
                        }
                        if (UtilValidate.isNotEmpty(productFeature.getString("featureDescription"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", productFeature.getString("featureTypeDesc"), "value", formatContentForCsv(productFeature.getString("featureDescription").toString())));
                        }
                    }

                    // Hazardous Material Code and ACES Applications.
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("isHazardousMaterial"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "Hazardous Material Code", "value", productFeatureAndAppl.getString("isHazardousMaterial")));
                    }
                    if (UtilValidate.isNotEmpty(productFeatureAndAppl.getString("acesApplications"))) {
                        attributeInfoList.add(UtilMisc.toMap("uniqueId", uniqueId, "key", "ACES Application", "value", productFeatureAndAppl.getString("acesApplications")));
                    }

                    if (attributeInfoList.size() > 1000) {
                        totalItems = totalItems + attributeInfoList.size();
                        dataFeedMap.put("dataFeedList", (List) attributeInfoList);
                        dataFeedMap.put("isHeader", isHeader);
                        FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                        attributeInfoList.clear();
                        TransactionUtil.commit(); //Commit T2

                        runningTx = TransactionUtil.suspend(); //Suspend T2

                        TransactionUtil.resume(defaultParentTx); //Resume T1

                        GenericValue dummyRecord = EntityQuery.use(delegator).from("DataManagerConfig").where("configId", "DUMMY").queryOne();

                        defaultParentTx = TransactionUtil.suspend(); //Suspend T1

                        TransactionUtil.resume(runningTx); //Resume T2

                        isHeader = false;
                    }
                }
            } else {
                Debug.logError("Unable to generate the data feed. No Product(s) in the system have Alliance Product ID set.", module);
            }
            TransactionUtil.commit(); //Commit T2

            TransactionUtil.resume(parentTx); //Resume T1
            if (attributeInfoList.size() > 0 || isHeader)     {
                totalItems = totalItems + attributeInfoList.size();
                dataFeedMap.put("dataFeedList", (List) attributeInfoList);
                dataFeedMap.put("isHeader", isHeader);
                FreeMarkerWorker.renderTemplateAtLocation(templateLocation, dataFeedMap, writer);
                attributeInfoList.clear();
            }
            writer.flush();
            writer.close();
            attributesDataFeed.put("dataFeedListSize", String.valueOf(totalItems));
        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation" + gee.getMessage());
        } catch (FileNotFoundException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (MalformedURLException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (IOException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (TemplateException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } finally {
            try {
                productFeatureAndAppls.close();

                try {
                    TransactionUtil.commit(); //Commit T1
                } catch (GenericTransactionException e) {
                    //log warning if error thrown when commiting transaction T1
                    Debug.logWarning("Long transaction", module);
                }

                if (defaultParentTx != null) TransactionUtil.resume(defaultParentTx); //Resume T0 - Default transaction
            } catch (GenericEntityException gee) {
                Debug.logError("Problem closing EntityListIterator" + gee.getMessage(), module);
                return ServiceUtil.returnError("Problem in closing EntityListIterator" + gee.getMessage());
            }
        }
        return attributesDataFeed;
    }

    public static Map<String, Object> prepareTimestampDataFeed(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();

        String logGroupId = (String) context.get("logGroupId");
        List<Map<String, Object>> timestampInfoList = new ArrayList<Map<String, Object>>();
        Map<String, Object> timestampFeed = null;
        Map<String, Object> timestampDataFeed = new HashMap<String, Object>();
        Map<String, Object> dataFeedMap = new HashMap<String, Object>();
        String outputDirLocation = (String) context.get("outputDirLocation");
        String fileName = (String) context.get("fileName");
        String templateLocation = (String) context.get("templateLocation");

        try {
            File file = new File(outputDirLocation + "/" + fileName);
            Charset cs = Charset.forName("UTF-8");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), cs));
            if (UtilValidate.isNotEmpty(logGroupId)) {
                List<GenericValue> feedConfigLogs = EntityQuery.use(delegator).from("DataFeedLog").where("feedLogGroupId", logGroupId).queryList();
                for (GenericValue feedConfigLog : feedConfigLogs) {
                    timestampFeed = new HashMap<>();
                    String feedLogId = feedConfigLog.getString("feedLogId");
                    GenericValue feedLogItem = EntityQuery.use(delegator).from("DataFeedLogItem").where("logId", feedLogId, "logItemTypeId", "TOTAL_ITEM_SUCCESS").queryOne();
                    if (feedLogItem != null) {
                        GenericValue feedConfig = EntityQuery.use(delegator).from("DataFeedConfig").where("feedConfigId", feedConfigLog.getString("feedConfigId")).queryOne();
                        timestampFeed.put("feedTemplate", feedConfig.getString("fileNamePattern"));
                        timestampFeed.put("feedListSize", feedLogItem.getString("value"));
                    }
                    timestampInfoList.add(timestampFeed);
                }

                String dataset = EntityUtilProperties.getPropertyValue("hawksearch","hawksearch.Dataset", delegator);
                GenericValue feedLogGroup = EntityQuery.use(delegator).from("DataFeedLog").where("feedLogId", logGroupId).queryOne();
                if (feedLogGroup != null) {
                    GenericValue feedConfigGroup = EntityQuery.use(delegator).from("DataFeedConfigGroup").where("feedConfigGroupId", feedLogGroup.getString("feedConfigGroupId")).queryOne();
                    if (feedConfigGroup != null) {
                        if (UtilValidate.isNotEmpty(feedConfigGroup.getString("lastFeedTimestamp"))) {
                            timestampDataFeed.put("feedType", "partial");
                            if (UtilValidate.isNotEmpty(dataset)) {
                                if ("Full".equals(dataset)) {
                                    timestampDataFeed.put("feedType", "full");
                                }
                            }
                        } else {
                            timestampDataFeed.put("feedType", "full");
                            if (UtilValidate.isNotEmpty(dataset)) {
                                if ("Partial".equals(dataset)) {
                                    timestampDataFeed.put("feedType", "partial");
                                }
                            }
                        }
                    }
                }
            }
            timestampDataFeed.put("dataFeedListSize", String.valueOf(timestampInfoList.size()));
            timestampDataFeed.put("dataFeedList", (List) timestampInfoList);
            FreeMarkerWorker.renderTemplateAtLocation(templateLocation, timestampDataFeed, writer);
            writer.flush();
            writer.close();

        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
        } catch (FileNotFoundException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (MalformedURLException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (IOException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        } catch (TemplateException e) {
            Debug.logError(e, module);
            return ServiceUtil.returnError(e.getMessage());
        }

        return timestampDataFeed;
    }

    // This method will format description(content) as per the CSV standards
    private static String formatContentForCsv(String description) {
        if (description.contains("\"")) {
            description = description.replace("\"", "\"\"");
        }
        StringBuilder csvFormattedString = new StringBuilder();
        csvFormattedString.insert(0, "\"");
        csvFormattedString.append(description);
        csvFormattedString.append("\"");

        return csvFormattedString.toString();
    }

    public static Map<String, Object> configureBrandForHawkSearch(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();

        String brandCategoryId = (String) context.get("brandCategoryId");
        String hawkSearchEnabled = (String) context.get("hawkSearchEnabled");
        String includeAllBrand = (String) context.get("includeAllBrand");
        String showBrands = (String) context.get("showBrands");
        String subbrandCategoryId = (String) context.get("subbrandCategoryId");
        String subBrandConfig = (String) context.get("subbrandConfig");
        GenericValue userLogin = (GenericValue) context.get("userLogin");
        Map<String, Object> results = ServiceUtil.returnSuccess();
        Map<String, Object> serviceCtx = null;

        try {
            if (UtilValidate.isNotEmpty(includeAllBrand)) {
                List<GenericValue> brands = null;
                if ("Configured".equals(showBrands)) {
                    brands = EntityQuery.use(delegator).from("ProductCategoryAndAttribute").where("productCategoryTypeId","AAIA_BRAND","attrValue", "Y").queryList();
                } else if ("NonConfigured".equals(showBrands) || "PartialConfigured".equals(showBrands)) {
                    brands = EntityQuery.use(delegator).from("ProductCategoryAndAttribute").where("productCategoryTypeId","AAIA_BRAND","attrValue", "N").queryList();
                    //Unconfigure all sub brands
                    List<GenericValue> configuredBrands = EntityQuery.use(delegator).from("ProductCategoryAndAttribute").where("attrName", "HAWKSEARCH_ENABLED", "attrValue", "Y", "productCategoryTypeId", "AAIA_SUBBRAND").queryList();
                    for (GenericValue configureBrand: configuredBrands) {
                        Map updateProductCatCtx = new HashMap<>();
                        updateProductCatCtx.put("productCategoryId", configureBrand.getString("productCategoryId"));
                        updateProductCatCtx.put("attrName", "HAWKSEARCH_ENABLED");
                        updateProductCatCtx.put("attrValue", "N");
                        updateProductCatCtx.put("userLogin", userLogin);
                        dispatcher.runSync("updateProductCategoryAttribute", updateProductCatCtx);
                    }
                } else {
                    //Unconfigure all sub brands
                    List<GenericValue> configuredBrands = EntityQuery.use(delegator).from("ProductCategoryAndAttribute").where("attrName", "HAWKSEARCH_ENABLED", "attrValue", "Y", "productCategoryTypeId", "AAIA_SUBBRAND").queryList();
                    for (GenericValue configureBrand: configuredBrands) {
                        Map updateProductCatCtx = new HashMap<>();
                        updateProductCatCtx.put("productCategoryId", configureBrand.getString("productCategoryId"));
                        updateProductCatCtx.put("attrName", "HAWKSEARCH_ENABLED");
                        updateProductCatCtx.put("attrValue", "N");
                        updateProductCatCtx.put("userLogin", userLogin);
                        dispatcher.runSync("updateProductCategoryAttribute", updateProductCatCtx);
                }
                    brands = EntityQuery.use(delegator).from("ProductCategoryAndAttribute").where("productCategoryTypeId", "AAIA_BRAND").queryList();
                }
                if (UtilValidate.isNotEmpty(brands) && !"PartialConfigured".equals(showBrands)) {
                    if ("Y".equals(includeAllBrand)) {
                        hawkSearchEnabled = "Y";
                    } else {
                        hawkSearchEnabled = "N";
                    }
                    for (GenericValue brand : brands) {
                        serviceCtx = new HashMap<>();
                        serviceCtx.put("productCategoryId", brand.getString("productCategoryId"));
                        GenericValue productCategoryAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", brand.getString("productCategoryId"), "attrName", "HAWKSEARCH_ENABLED").queryOne();
                        if (productCategoryAttribute == null) {
                            serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                            serviceCtx.put("attrValue", hawkSearchEnabled);
                            serviceCtx.put("userLogin", userLogin);
                            dispatcher.runSync("createProductCategoryAttribute", serviceCtx);
                        } else {
                            serviceCtx.put("attrName", productCategoryAttribute.getString("attrName"));
                            serviceCtx.put("attrValue", hawkSearchEnabled);
                            serviceCtx.put("userLogin", userLogin);
                            dispatcher.runSync("updateProductCategoryAttribute", serviceCtx);
                        }
                    }
                    if ("Y".equals(includeAllBrand)) {
                        results.put(ModelService.SUCCESS_MESSAGE, "All Brands are enabled for Hawk Search.");
                    } else {
                        results.put(ModelService.SUCCESS_MESSAGE, "All Brands are disabled from Hawk Search.");
                    }
                }
            } else if (UtilValidate.isNotEmpty(brandCategoryId) && UtilValidate.isEmpty(subbrandCategoryId)) {
                List<GenericValue> subBrandList = EntityQuery.use(delegator).from("ProductCategory").where("primaryParentCategoryId", brandCategoryId, "productCategoryTypeId", "AAIA_SUBBRAND").queryList();
                if (UtilValidate.isNotEmpty(subBrandList)) {
                    for (GenericValue subBrand: subBrandList) {
                        GenericValue productCatAttr = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", subBrand.getString("productCategoryId"), "attrName", "HAWKSEARCH_ENABLED", "attrValue", "Y").queryOne();
                        if (productCatAttr != null) {
                            serviceCtx = new HashMap<>();
                            serviceCtx.put("attrValue", "N");
                            serviceCtx.put("productCategoryId", productCatAttr.getString("productCategoryId"));
                            serviceCtx.put("attrName", productCatAttr.getString("attrName"));
                            serviceCtx.put("userLogin", userLogin);
                            dispatcher.runSync("updateProductCategoryAttribute", serviceCtx);
                        }
                    }
                }
            GenericValue brand = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", brandCategoryId).queryOne();
            if (brand != null) {
                serviceCtx = new HashMap<>();
                serviceCtx.put("productCategoryId", brandCategoryId);
                GenericValue productCategoryAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", brandCategoryId, "attrName", "HAWKSEARCH_ENABLED").queryOne();
                if (productCategoryAttribute == null) {
                    serviceCtx.put("attrValue", hawkSearchEnabled);
                    serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                    serviceCtx.put("userLogin", userLogin);
                    dispatcher.runSync("createProductCategoryAttribute", serviceCtx);
                } else {
                    serviceCtx.put("attrValue", hawkSearchEnabled);
                    serviceCtx.put("attrName", productCategoryAttribute.getString("attrName"));
                    serviceCtx.put("userLogin", userLogin);
                    dispatcher.runSync("updateProductCategoryAttribute", serviceCtx);
                }
                if ("Y".equals(hawkSearchEnabled)) {
                    results.put(ModelService.SUCCESS_MESSAGE, "The brand " + brand.getString("description")
                        + " [" + brand.getString("categoryName") + "] " + "is now enabled for Hawk Search Feed.");
                } else {
                    results.put(ModelService.SUCCESS_MESSAGE, "The brand " + brand.getString("description")
                        + " [" + brand.getString("categoryName") + "] " + "is now disabled from Hawk Search Feed.");
                }
            }
            } else if (UtilValidate.isNotEmpty(subbrandCategoryId)) {
                GenericValue subBrand = EntityQuery.use(delegator).from("ProductCategory").where("productCategoryId", subbrandCategoryId).queryOne();
                if (subBrand != null) {
                    GenericValue parentBrand = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", subBrand.getString("primaryParentCategoryId"),"attrName","HAWKSEARCH_ENABLED","attrValue","Y").queryFirst();
                    if (parentBrand != null && UtilValidate.isNotEmpty(subBrandConfig) && "N".equals(subBrandConfig)) {
                        if (UtilValidate.isNotEmpty(subBrand.getString("primaryParentCategoryId"))) {
                            GenericValue productCategoryAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", subBrand.getString("primaryParentCategoryId")).queryOne();
                            if (productCategoryAttribute != null) {
                                Map productCatCtx = new HashMap<>();
                                productCatCtx.put("productCategoryId", productCategoryAttribute.getString("productCategoryId"));
                                productCatCtx.put("attrValue", "N");
                                productCatCtx.put("attrName", productCategoryAttribute.getString("attrName"));
                                productCatCtx.put("userLogin", userLogin);
                                dispatcher.runSync("updateProductCategoryAttribute", productCatCtx);
                            }
                            List<GenericValue> productCategoryList = EntityQuery.use(delegator).from("ProductCategory").where("primaryParentCategoryId", subBrand.getString("primaryParentCategoryId")).queryList();
                            if (UtilValidate.isNotEmpty(productCategoryList)) {
                                productCategoryList.remove(subBrand);
                                for (GenericValue productCat: productCategoryList) {
                                    Map prodCatAttr = new HashMap<>();
                                    prodCatAttr.put("productCategoryId", productCat.getString("productCategoryId"));
                                    GenericValue prodCatAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", productCat.getString("productCategoryId"), "attrName", "HAWKSEARCH_ENABLED").queryFirst();
                                    if (prodCatAttribute == null) {
                                        prodCatAttr.put("attrName", "HAWKSEARCH_ENABLED");
                                        prodCatAttr.put("attrValue", "Y");
                                        prodCatAttr.put("userLogin", userLogin);
                                        dispatcher.runSync("createProductCategoryAttribute", prodCatAttr);
                                    } else {
                                        prodCatAttr.put("attrValue", "Y");
                                        prodCatAttr.put("attrName", "HAWKSEARCH_ENABLED");
                                        prodCatAttr.put("userLogin", userLogin);
                                        dispatcher.runSync("updateProductCategoryAttribute", prodCatAttr);
                                    }
                                }
                            }
                        }
                    } else {
                    serviceCtx = new HashMap<>();
                    serviceCtx.put("productCategoryId", subbrandCategoryId);
                    GenericValue productCategoryAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", subbrandCategoryId, "attrName", "HAWKSEARCH_ENABLED").queryOne();
                    if (productCategoryAttribute == null) {
                        serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                        serviceCtx.put("attrValue", subBrandConfig);
                        serviceCtx.put("userLogin", userLogin);
                        dispatcher.runSync("createProductCategoryAttribute", serviceCtx);
                    } else {
                        serviceCtx.put("attrValue", subBrandConfig);
                        serviceCtx.put("attrName", productCategoryAttribute.getString("attrName"));
                        serviceCtx.put("userLogin", userLogin);
                        dispatcher.runSync("updateProductCategoryAttribute", serviceCtx);
                    }
                    }
                    // Check if all are configured.
                    List<GenericValue> subBrandList = EntityQuery.use(delegator).select("productCategoryId").from("ProductCategory").where("primaryParentCategoryId", subBrand.getString("primaryParentCategoryId")).queryList();
                    boolean x = true;
                    if (subBrandList != null) {
                        for (GenericValue configureSubBrand: subBrandList) {
                            GenericValue subBrandAttribute = EntityQuery.use(delegator).select("productCategoryId").from("ProductCategoryAttribute").where("productCategoryId", configureSubBrand.getString("productCategoryId"), "attrValue", "Y").queryFirst();
                            if (subBrandAttribute == null) {
                                x = false;
                                break;
                            }
                        }
                    } else {
                        x = false;
                    }
                    // If all sub brands of the brand are configured, unconfigure them all and configure the brand itself
                    if (x == true) {
                        for (GenericValue configureSubBrand: subBrandList) {
                            serviceCtx = new HashMap<>();
                            serviceCtx.put("productCategoryId", configureSubBrand.getString("productCategoryId"));
                            GenericValue productCategoryAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", configureSubBrand.getString("productCategoryId"), "attrName", "HAWKSEARCH_ENABLED").queryOne();
                            if (productCategoryAttribute == null) {
                                serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                                serviceCtx.put("attrValue", "N");
                                serviceCtx.put("userLogin", userLogin);
                                dispatcher.runSync("createProductCategoryAttribute", serviceCtx);
                            } else {
                                serviceCtx.put("attrValue", "N");
                                serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                                serviceCtx.put("userLogin", userLogin);
                                dispatcher.runSync("updateProductCategoryAttribute", serviceCtx);
                            }
                        }

                            serviceCtx = new HashMap<>();
                            serviceCtx.put("productCategoryId", subBrand.getString("primaryParentCategoryId"));
                            GenericValue productCategoryAttribute = EntityQuery.use(delegator).from("ProductCategoryAttribute").where("productCategoryId", subBrand.getString("primaryParentCategoryId"), "attrName", "HAWKSEARCH_ENABLED").queryOne();
                            if (productCategoryAttribute == null) {
                                serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                                serviceCtx.put("attrValue", "Y");
                                serviceCtx.put("userLogin", userLogin);
                                dispatcher.runSync("createProductCategoryAttribute", serviceCtx);
                            } else {
                                serviceCtx.put("attrValue", "Y");
                                serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                                serviceCtx.put("userLogin", userLogin);
                                dispatcher.runSync("updateProductCategoryAttribute", serviceCtx);
                            }

                    }
                }
            }

        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation" + gee.getMessage());
        } catch (GenericServiceException gse) {
            Debug.logError(gse.getMessage(), module);
            return ServiceUtil.returnError(gse.getMessage());
        }

        return results;
    }

    public static Map<String, Object> configureWDForHawkSearch(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();

        String memberId = (String) context.get("memberId");
        String hawkSearchEnabled = (String) context.get("hawkSearchEnabled");
        String includeAllWd = (String) context.get("includeAllWd");
        GenericValue userLogin = (GenericValue) context.get("userLogin");

        Map<String, Object> results = ServiceUtil.returnSuccess();
        Map<String, Object> serviceCtx = null;

        try {
            if (UtilValidate.isNotEmpty(includeAllWd)) {
                List<GenericValue> members = EntityQuery.use(delegator).from("PartyPartyRoleAndPartyGroup").where("roleTypeId", "MEMBER").queryList();
                if (UtilValidate.isNotEmpty(members)) {
                    if ("Y".equals(includeAllWd)) {
                        hawkSearchEnabled = "Y";
                    } else {
                        hawkSearchEnabled = "N";
                    }
                    for (GenericValue member : members) {
                        GenericValue partyAttribute = EntityQuery.use(delegator).from("PartyAttribute").where("partyId", member.getString("partyId"), "attrName", "HAWKSEARCH_ENABLED").queryOne();
                        serviceCtx = new HashMap<>();
                        serviceCtx.put("partyId", member.getString("partyId"));
                        if (partyAttribute == null) {
                            serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                            serviceCtx.put("attrValue", hawkSearchEnabled);
                            serviceCtx.put("userLogin", userLogin);
                            dispatcher.runSync("createPartyAttribute", serviceCtx);
                        } else {
                            serviceCtx.put("attrName", partyAttribute.getString("attrName"));
                            serviceCtx.put("attrValue", hawkSearchEnabled);
                            serviceCtx.put("userLogin", userLogin);
                            dispatcher.runSync("updatePartyAttribute", serviceCtx);
                        }
                    }
                    if ("Y".equals(includeAllWd)) {
                        hawkSearchEnabled = "Y";
                        results.put(ModelService.SUCCESS_MESSAGE, "All Warehouse Distributors are enabled for Hawk Search.");
                    } else {
                        hawkSearchEnabled = "N";
                        results.put(ModelService.SUCCESS_MESSAGE, "All Warehouse Distributors are disabled from Hawk Search.");
                    }
                }
            } else {
            GenericValue party = EntityQuery.use(delegator).from("PartyGroup").where("partyId", memberId).queryOne();
            if (party != null) {
                GenericValue member = EntityQuery.use(delegator).from("PartyRole").where("partyId", memberId, "roleTypeId", "MEMBER").queryOne();
                if (member != null) {
                    serviceCtx = new HashMap<>();
                    serviceCtx.put("partyId", memberId);
                    GenericValue partyAttribute = EntityQuery.use(delegator).from("PartyAttribute").where("partyId", memberId, "attrName", "HAWKSEARCH_ENABLED").queryOne();
                    if (partyAttribute == null) {
                        serviceCtx.put("attrName", "HAWKSEARCH_ENABLED");
                        serviceCtx.put("attrValue", hawkSearchEnabled);
                        serviceCtx.put("userLogin", userLogin);
                        dispatcher.runSync("createPartyAttribute", serviceCtx);
                    } else {
                        serviceCtx.put("attrName", partyAttribute.getString("attrName"));
                        serviceCtx.put("attrValue", hawkSearchEnabled);
                        serviceCtx.put("userLogin", userLogin);
                        dispatcher.runSync("updatePartyAttribute", serviceCtx);
                    }
                    if ("Y".equals(hawkSearchEnabled)) {
                        results.put(ModelService.SUCCESS_MESSAGE, "The member " + party.getString("groupName")
                            + " [" + party.getString("partyId") + "] " + "is now enabled for Hawk Search Feed.");
                    } else {
                        results.put(ModelService.SUCCESS_MESSAGE, "The member " + party.getString("groupName")
                            + " [" + party.getString("partyId") + "] " + "is now disabled from Hawk Search Feed.");
                    }
                }
            }
            }
        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation" + gee.getMessage());
        } catch (GenericServiceException gse) {
            Debug.logError(gse.getMessage(), module);
            return ServiceUtil.returnError(gse.getMessage());
        }

        return results;
    }

    public static Map<String, Object> includeAllProducts(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();
        String includeAllProduct = (String) context.get("includeAllProduct");
        String includeProducts = EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch.ALL_PRODUCTS", delegator);
        Map result = new HashMap();
            try {
                GenericValue systemProperty = EntityQuery.use(delegator).from("SystemProperty").where("systemResourceId", "hawksearch", "systemPropertyId", "hawksearch.ALL_PRODUCTS").queryOne();
                if (systemProperty != null) {
                    systemProperty.put("systemPropertyValue", includeAllProduct);
                    systemProperty.store();
                }
            } catch (GenericEntityException gse) {
                Debug.logError(gse.getMessage(), module);
                return ServiceUtil.returnError(gse.getMessage());
        }
        if ("Y".equals(includeAllProduct)) {
           return ServiceUtil.returnSuccess("All Products for the configured brands are enabled for Hawk Search.");
        } else {
           return ServiceUtil.returnSuccess("Only modified products for the configured brands are enabled for Hawk Search.");
        }
    }

    public static Map<String, Object> updateDatasetValueOfHawk(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();
            GenericValue userLogin = (GenericValue) context.get("userLogin");

        String modifiedDataset = (String) context.get("modifiedDataset");
        try {
            GenericValue systemProperty = EntityQuery.use(delegator).from("SystemProperty").where("systemResourceId", "hawksearch", "systemPropertyId", "hawksearch.Dataset").queryOne();
            if (systemProperty != null) {
                systemProperty.put("systemPropertyValue", modifiedDataset);
                systemProperty.store();
            }

            String includeProducts = EntityUtilProperties.getPropertyValue("hawksearch", "hawksearch.ALL_PRODUCTS", delegator);
            if (UtilValidate.isNotEmpty(includeProducts) && "N".equals(includeProducts) && "Full".equals(modifiedDataset)) {
                Map<String, Object> serviceCtx = new HashMap<String, Object>();
                serviceCtx.put("includeAllProduct", "Y");
                serviceCtx.put("userLogin", userLogin);
                dispatcher.runSync("includeAllProducts", serviceCtx);
            }
        } catch (GenericEntityException gee) {
            Debug.logError(gee.getMessage(), module);
            return ServiceUtil.returnError(gee.getMessage());
        } catch (GenericServiceException gse) {
            Debug.logError(gse.getMessage(), module);
            return ServiceUtil.returnError(gse.getMessage());
        }
        if ("Full".equals(modifiedDataset)) {
           return ServiceUtil.returnSuccess("Dataset value for timestamp feed changed to full.");
        } else {
           return ServiceUtil.returnSuccess("Dataset value for timestamp feed changed to partial.");
        }
    }

    public static Map<String, Object> archiveHawkDataFeed(DispatchContext dctx, Map<String, Object> context) {
        Delegator delegator = dctx.getDelegator();

        try {
            GenericValue dataFeedConfigGroup = EntityQuery.use(delegator).from("DataFeedConfigGroup").where("feedConfigGroupId", "1000").queryOne();
            if (dataFeedConfigGroup != null) {
                GenericValue dataFeedConfig = EntityQuery.use(delegator).from("DataFeedConfig").where("feedConfigGroupId", dataFeedConfigGroup.getString("feedConfigGroupId")).queryFirst();
                if (dataFeedConfig != null) {
                    String username = dataFeedConfig.getString("sftpUserName");
                    String password = dataFeedConfig.getString("sftpPassword");
                    String sftpUrl = dataFeedConfig.getString("sftpUrl");
                    String sftpDirectory = dataFeedConfig.getString("sftpDirectory");
                    if (UtilValidate.isNotEmpty(sftpUrl) && UtilValidate.isNotEmpty(username) && UtilValidate.isNotEmpty(password) && UtilValidate.isNotEmpty(sftpDirectory)) {
                        Session session = null;
                        Channel channel = null;
                        ChannelSftp channelSftp = null;
                        Debug.logInfo("=====Preparing host information for SFTP.=====", module);
                        JSch jsch = new JSch();
                        session = jsch.getSession(username, sftpUrl, 22);
                        session.setPassword(password);
                        java.util.Properties config = new java.util.Properties();
                        config.put("StrictHostKeyChecking", "no");
                        session.setConfig(config);
                        session.connect();
                        channel = session.openChannel("sftp");
                        channel.connect();
                        Debug.logInfo("=====SFTP Channel opened and connected.=====", module);
                        channelSftp = (ChannelSftp) channel;
                        channelSftp.cd(sftpDirectory);
                        String archiveFolder = sftpDirectory + "archive_feeds";
                        String feedPath = sftpDirectory + "hawk_datafeeds.zip";

                        // Identify the hawk_datafeeds.zip exists or not.
                        try {
                            channelSftp.lstat(feedPath);
                        } catch (SftpException e) {
                            Debug.logInfo("=====No data feed to archive.=====", module);
                            return ServiceUtil.returnSuccess("=====No hawk_datafeeds.zip file to archive.=====");
                        }
                        // If the execution continues, then the file exists
                        // TODO: Fetch the status of the last data feed on Hawk

                        // TODO: Get the last Feed Time of the Hawk Feed and compare with the last Time feed was consumed by Hawk Team
                        SftpATTRS attrs = null;
                        Channel channel1 = session.openChannel("sftp");
                        channel1.connect();
                        ChannelSftp sftpChannel1 = (ChannelSftp) channel1;
                        try {
                            attrs = sftpChannel1.stat(archiveFolder);
                        } catch (Exception e) {
                            Debug.logInfo("Archive folder at " + archiveFolder + " not found", module);
                        }
                        if (attrs == null) {
                            sftpChannel1.mkdir(archiveFolder);
                            Debug.logInfo("========The Archive folder is now created at " + archiveFolder, module);
                        }
                        sftpChannel1.cd(archiveFolder);
                        try {
                            Date now = new Date();
                            SimpleDateFormat sdf = new SimpleDateFormat ("yyyyMMddHHmmss");
                            String fixedTime = sdf.format(now);
                            sftpChannel1.rename(feedPath, archiveFolder + "/" + fixedTime + "_hawk_datafeeds.zip");
                        } catch (SftpException e) {
                            Debug.logError("=====Error in archiving the data feed file.=====" + e.getMessage(), module);
                            return ServiceUtil.returnError("=====Error in archiving the data feed file.=====" + e.getMessage());
                        }
                        Debug.logInfo("=====Data Feed archived successfully.=====", module);
                    }
                }
            }

        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
            return ServiceUtil.returnError("Error occurred during entity operation" + gee.getMessage());
        } catch (JSchException e) {
            Debug.logError("Error occurred Jsch " + e.getMessage(), module);
            return ServiceUtil.returnError("Error occurred Jsch " + e.getMessage());
        } catch (SftpException e) {
            Debug.logError("Error occurred Sftp " + e.getMessage(), module);
            return ServiceUtil.returnError("Error occurred Sftp " + e.getMessage());
        }
        return ServiceUtil.returnSuccess();
    }

    public static BigDecimal getProductRetailPrice(Delegator delegator, String productId) {
        BigDecimal retailPrice = BigDecimal.ZERO;

        try {
        GenericValue zapPrice = EntityQuery.use(delegator).from("ProductPrice")
                .where("productId", productId, "productPriceTypeId", "LIST_PRICE", "priceCode", "ZAP").filterByDate().queryFirst();
        if (zapPrice != null && UtilValidate.isNotEmpty(zapPrice.getBigDecimal("price"))) {
            retailPrice = zapPrice.getBigDecimal("price").multiply(new BigDecimal(2.5));
        } else {
            GenericValue listPrice = EntityQuery.use(delegator).from("ProductPrice")
                    .where("productId", productId, "productPriceTypeId", "LIST_PRICE", "priceCode", "LST").filterByDate().queryFirst();
            if (listPrice != null && UtilValidate.isNotEmpty(listPrice.getBigDecimal("price"))) {
                retailPrice = listPrice.getBigDecimal("price");
            } else {
                GenericValue jobberPrice = EntityQuery.use(delegator).from("ProductPrice")
                        .where("productId", productId, "productPriceTypeId", "JOBBER_PRICE").filterByDate().queryFirst();
                if (jobberPrice != null && UtilValidate.isNotEmpty(jobberPrice.getBigDecimal("price"))) {
                    retailPrice = jobberPrice.getBigDecimal("price").multiply(new BigDecimal(2));
                } else {
                    GenericValue quotePrice = EntityQuery.use(delegator).from("ProductPrice")
                            .where("productId", productId, "productPriceTypeId", "QOT").filterByDate().queryFirst();
                    if (quotePrice != null && UtilValidate.isNotEmpty(quotePrice.getBigDecimal("price"))) {
                        retailPrice = quotePrice.getBigDecimal("price").multiply(new BigDecimal(2.8));
                    } else {
                        GenericValue userPrice = EntityQuery.use(delegator).from("ProductPrice")
                                .where("productId", productId, "productPriceTypeId", "USR").filterByDate().queryFirst();
                        if (userPrice != null && UtilValidate.isNotEmpty(userPrice.getBigDecimal("price"))) {
                            retailPrice = userPrice.getBigDecimal("price").multiply(new BigDecimal(2.8));
                        } else {
                            GenericValue wdPrice = EntityQuery.use(delegator).from("ProductPrice")
                                    .where("productId", productId, "productPriceTypeId", "WD1").filterByDate().queryFirst();
                            if (wdPrice != null && UtilValidate.isNotEmpty(wdPrice.getBigDecimal("price"))) {
                                retailPrice = wdPrice.getBigDecimal("price").multiply(new BigDecimal(2.8));
                            } else {
                                retailPrice = new BigDecimal(1234.56);
                            }
                        }
                    }
                }
            }
        }
        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
        }
        return retailPrice;
    }

    public static String getProductThumbnail (Delegator delegator, String productId) {
        String imageUrl = null;
        try {
        //Code to generate image url.
        //Code to send the thumbnail associated with 400 asset width image to hawk.
            EntityCondition condition = EntityCondition.makeCondition(UtilMisc.toList(
                    EntityCondition.makeCondition("productId", EntityOperator.EQUALS, productId),
                    EntityCondition.makeCondition("contentTypeId", EntityOperator.EQUALS, "P04"),
                    EntityCondition.makeCondition("contentName", EntityOperator.NOT_EQUAL, null),
                    EntityCondition.makeCondition("contentAssocTypeId", EntityOperator.EQUALS, "ALT_SIZE_THUMBNAIL")), EntityOperator.AND);
            GenericValue productContentInfo = EntityQuery.use(delegator).from("ProductContentAndAssoc").where(condition).queryFirst();
            if (productContentInfo != null) {
                GenericValue dataResourceMetaData = EntityQuery.use(delegator).from("DataResourceMetaData")
                        .where("dataResourceId", productContentInfo.getString("toDataResourceId"), "metaDataPredicateId", "url").queryFirst();
                if (dataResourceMetaData != null) {
                    if (UtilValidate.isNotEmpty(dataResourceMetaData.getString("metaDataValue"))) {
                        imageUrl = dataResourceMetaData.getString("metaDataValue");
                    }
                }
            }

            //Code to send the thumbnail associated with 400 asset height image to hawk.
            if (imageUrl == null) {
                EntityCondition imageCondition = EntityCondition.makeCondition(UtilMisc.toList(
                        EntityCondition.makeCondition("productId", EntityOperator.EQUALS, productId),
                        EntityCondition.makeCondition("contentTypeId", EntityOperator.EQUALS, "DOCUMENT"),
                        EntityCondition.makeCondition("dataResourceId", EntityOperator.EQUALS, null),
                        EntityCondition.makeCondition("contentAssocTypeId", EntityOperator.EQUALS, "ALT_SIZE")), EntityOperator.AND);
                List<GenericValue> productContentAssocs = EntityQuery.use(delegator).from("ProductContentAndAssoc").where(imageCondition).queryList();
                for (GenericValue productContentAssoc : productContentAssocs) {
                    if (productContentAssoc != null && UtilValidate.isNotEmpty(productContentAssoc.getString("toDataResourceId"))) {
                        GenericValue dataResourceMetaData = EntityQuery.use(delegator).from("DataResourceMetaData").where("dataResourceId", productContentAssoc.getString("toDataResourceId"), "metaDataPredicateId", "asset-height", "metaDataValue", "400").queryFirst();
                        if (dataResourceMetaData != null) {
                            GenericValue contentAssoc = EntityQuery.use(delegator).from("ContentAssocViewTo").where("contentIdStart", productContentAssoc.getString("contentId"), "caContentAssocTypeId", "ALT_SIZE_THUMBNAIL").queryFirst();
                            if (contentAssoc != null) {
                                GenericValue metaDataUrl = EntityQuery.use(delegator).from("DataResourceMetaData").where("dataResourceId", contentAssoc.getString("dataResourceId"), "metaDataPredicateId", "url").queryFirst();
                                if (metaDataUrl != null) {
                                    imageUrl = metaDataUrl.getString("metaDataValue");
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // Full size image with FRO orientation-view
            if (imageUrl == null) {
                HashMap<String, Integer> contentAssocAndMetaDataMap = new HashMap<String, Integer>();
                EntityCondition orientationViewCond = EntityCondition.makeCondition(UtilMisc.toList(
                        EntityCondition.makeCondition("productId", EntityOperator.EQUALS, productId),
                        EntityCondition.makeCondition("contentName", EntityOperator.NOT_EQUAL, null),
                        EntityCondition.makeCondition("contentAssocTypeId", EntityOperator.EQUALS, "ALT_SIZE"),
                        EntityCondition.makeCondition("metaDataPredicateId", EntityOperator.EQUALS, "orientation-view"),
                        EntityCondition.makeCondition("metaDataValue", EntityOperator.EQUALS, "FRO")), EntityOperator.AND);
                List<GenericValue> productContentAssocAndMetaDataList = EntityQuery.use(delegator).from("ProductContentAssocAndMetaData").where(orientationViewCond).queryList();

                for (GenericValue productContentAssocAndMetaData: productContentAssocAndMetaDataList) {
                    EntityCondition assetWidthCond = EntityCondition.makeCondition(UtilMisc.toList(
                            EntityCondition.makeCondition("toDataResourceId", EntityOperator.EQUALS, productContentAssocAndMetaData.getString("toDataResourceId")),
                            EntityCondition.makeCondition("metaDataPredicateId", EntityOperator.EQUALS, "asset-width")), EntityOperator.AND);
                    GenericValue assetWidth = EntityQuery.use(delegator).from("ProductContentAssocAndMetaData").where(assetWidthCond).queryFirst();
                    if (assetWidth != null) {
                        contentAssocAndMetaDataMap.put(assetWidth.getString("toDataResourceId"), Integer.parseInt(assetWidth.getString("metaDataValue")));
                    }
                }

                // Iterating over the map to find key with max value
                String maxWidthImg = "";
                for (Map.Entry entry : contentAssocAndMetaDataMap.entrySet()) {
                    if (entry.getValue() == Collections.max(contentAssocAndMetaDataMap.values())) {
                        maxWidthImg = (String) entry.getKey();
                    }
                }

                if (UtilValidate.isNotEmpty(maxWidthImg)) {
                    GenericValue assetId = EntityQuery.use(delegator).from("ProductContentAssocAndMetaData").where("toDataResourceId", maxWidthImg).queryFirst();
                    if (assetId != null && UtilValidate.isNotEmpty(assetId.getString("contentName"))) {
                         EntityCondition maxAssetImg = EntityCondition.makeCondition(UtilMisc.toList(
                                EntityCondition.makeCondition("productId", EntityOperator.EQUALS, productId),
                                EntityCondition.makeCondition("contentName", EntityOperator.EQUALS, assetId.getString("contentName")),
                                EntityCondition.makeCondition("contentAssocTypeId", EntityOperator.EQUALS, "ALT_SIZE_THUMBNAIL")), EntityOperator.AND);
                        GenericValue contentAndAssoc = EntityQuery.use(delegator).from("ProductContentAndAssoc").where(maxAssetImg).queryFirst();
                        if (contentAndAssoc != null && UtilValidate.isNotEmpty(contentAndAssoc.getString("toDataResourceId"))) {
                            GenericValue dataResourceMetaData = EntityQuery.use(delegator).from("DataResourceMetaData").where("dataResourceId", contentAndAssoc.getString("toDataResourceId"), "metaDataPredicateId", "url").queryFirst();
                            if (dataResourceMetaData != null) {
                                imageUrl = dataResourceMetaData.getString("metaDataValue");
                            }
                        }
                     }
                }
            }
            // Full size image
            if (imageUrl == null) {
                EntityCondition maxImageCond = EntityCondition.makeCondition(UtilMisc.toList(
                        EntityCondition.makeCondition("productId", EntityOperator.EQUALS, productId),
                        EntityCondition.makeCondition("contentName", EntityOperator.NOT_EQUAL, null),
                        EntityCondition.makeCondition("contentAssocTypeId", EntityOperator.EQUALS, "ALT_SIZE"),
                        EntityCondition.makeCondition("metaDataPredicateId", EntityOperator.EQUALS, "asset-width")), EntityOperator.AND);
                GenericValue productContentAssocAndMetaData = EntityQuery.use(delegator).from("ProductContentAssocAndMetaData").where(maxImageCond).orderBy("-metaDataValue").queryFirst();
                if (productContentAssocAndMetaData != null) {
                    GenericValue assetId = EntityQuery.use(delegator).from("ProductContentAssocAndMetaData").where("toDataResourceId", productContentAssocAndMetaData.getString("toDataResourceId")).queryFirst();
                    if (assetId != null && UtilValidate.isNotEmpty(assetId.getString("contentName"))) {
                         EntityCondition maxAssetImg = EntityCondition.makeCondition(UtilMisc.toList(
                                EntityCondition.makeCondition("productId", EntityOperator.EQUALS, productId),
                                EntityCondition.makeCondition("contentName", EntityOperator.EQUALS, assetId.getString("contentName")),
                                EntityCondition.makeCondition("contentAssocTypeId", EntityOperator.EQUALS, "ALT_SIZE_THUMBNAIL")), EntityOperator.AND);
                         GenericValue contentAndAssoc = EntityQuery.use(delegator).from("ProductContentAndAssoc").where(maxAssetImg).queryFirst();
                        if (contentAndAssoc != null && UtilValidate.isNotEmpty(contentAndAssoc.getString("toDataResourceId"))) {
                            GenericValue dataResourceMetaData = EntityQuery.use(delegator).from("DataResourceMetaData").where("dataResourceId", contentAndAssoc.getString("toDataResourceId"), "metaDataPredicateId", "url").queryFirst();
                            if (dataResourceMetaData != null) {
                                imageUrl = dataResourceMetaData.getString("metaDataValue");
                            }
                        }
                    }
                }
            }
        } catch (GenericEntityException gee) {
            Debug.logError("Error occurred during entity operation :" + gee.getMessage(), module);
        }
        return imageUrl;
    }
}