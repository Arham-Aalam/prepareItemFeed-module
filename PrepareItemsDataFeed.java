
import java.io.FileInputStream;
import java.sql.*;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedList;
import java.lang.Integer;
import java.lang.StringBuilder;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;


public class PrepareItemsDataFeed {

    static Connection con;
    static Connection con2;
    static final String DB_NAME = "aapa";
    static final String TABLE_NAME = "product";
    static PreparedStatement statement;

    public static void main(String[] arg) {

    String filePath = "", fileName = "", zipName = "", productId = "", partNumber = "", name = "", terminologyName = "", imageUrl = "", brandName = "", brandCode = "", subBrandCode = "";
    String marketDesc, descriptionLong;
    StringBuilder descriptionShort = null, sku = null;
    Scanner sc;
    BufferedWriter bufferWrite;

    ResultSet productFeatureAndAppls = null, product = null, subBrand = null, goodCore = null, jobberCore=null, wdCore=null;
    ResultSet dataResourceId = null;
    ResultSet textDataProduct = null;
    ResultSet terminology = null;
    ResultSet brand = null;
    ResultSet electronicText = null;
    ResultSet textDataMarket = null;
    ResultSet jobberPrice = null;
    ResultSet quotePrice = null;
    ResultSet userPrice = null;
    ResultSet wdPrice = null;
    Map<String, Object> itemInfoMap = null;
    List<BigDecimal> priceList;
    BigDecimal retailPrice;
    Timestamp nowTimestamp;
    Timestamp lastFeedTimestamp;


    String includeProducts = "N";  // Also test for Y
        
        nowTimestamp = Timestamp.valueOf(java.time.LocalDate.of(2018, 6, 25).atStartOfDay());
        lastFeedTimestamp = Timestamp.valueOf(java.time.LocalDate.of(2018, 4, 19).atStartOfDay());
        sc = new Scanner(System.in);

        try {
            BufferedReader buffReader = new BufferedReader(new FileReader("example.txt"));
            filePath = buffReader.readLine();
            fileName = buffReader.readLine();
            zipName = buffReader.readLine();
            buffReader.close();
        } catch (Exception e) {
             e.printStackTrace();
        }


        try {
            //file objects
            bufferWrite = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(new File(filePath + "/" + fileName + ".txt")), StandardCharsets.UTF_8));
            bufferWrite.write("unique_id"+"    "+"name"+ "    "+"url_detail"+"    "+"image"+"    "+"price_retail"+"    "+"price_sale" +"    "+ "price_special" +"    "+ "group_id" +"    "+ "description_short" +"    "+ "description_long" +"    "+ "sku" +"    "+ "sort_default" +"    "+ "sort_rating" +"    "+ "item_operation\n");

            connectToDB2();
            connectToDB();
            int count = 0;
            if(includeProducts.equals("N")) {  //equal to 'N' && not empty  12345
                productFeatureAndAppls = queryData1("SELECT * FROM (SELECT P.PRODUCT_ID, P.SALES_DISCONTINUATION_DATE, P.ALLIANCE_PRODUCT_ID, P.IS_HAWK_INDEXED, P.HAWK_INDEXED_DATE, P.STATUS_ID, P.PRODUCT_TYPE_ID, P.DOMAIN_PARTY_ID, P.PRIMARY_PRODUCT_CATEGORY_ID, P.LAST_MODIFIED_DATE, P.PRODUCT_UPDATE_DATE, P.PART_NUMBER, P.AAIA_BRAND_ID, P.AAIA_SUB_BRAND_ID, P.NATIONAL_POPULARITY_CODE, P.STD_PART_NUMBER, PCA.ATTR_NAME, PCA.ATTR_VALUE, PCA.PRODUCT_CATEGORY_ID FROM PRODUCT P LEFT OUTER JOIN PRODUCT_CATEGORY_ATTRIBUTE PCA ON P.AAIA_BRAND_ID = PCA.PRODUCT_CATEGORY_ID LEFT OUTER JOIN PRODUCT_CATEGORY_ATTRIBUTE PCAS ON P.AAIA_SUB_BRAND_ID = PCAS.PRODUCT_CATEGORY_ID WHERE (((((PCAS.ATTR_NAME = 'HAWKSEARCH_ENABLED' OR PCA.ATTR_NAME = 'HAWKSEARCH_ENABLED') AND (PCAS.ATTR_VALUE = 'Y' OR PCA.ATTR_VALUE = 'Y') AND P.ALLIANCE_PRODUCT_ID IS NOT NULL AND P.DOMAIN_PARTY_ID = 'AAPA')))) ORDER BY P.PRODUCT_ID ASC, PCA.ATTR_NAME ASC, PCA.PRODUCT_CATEGORY_ID ASC) AS Products WHERE (((last_modified_date > "+ (new Date(lastFeedTimestamp.getTime())) +") OR (product_update_date > "+ (new Date(lastFeedTimestamp.getTime())) +")) AND ((((status_id IS NOT NULL) OR  (status_id != 'PRODUCT_STS_7')  OR (status_id != 'PRODUCT_STS_8') OR (status_id != 'PRODUCT_STS_9')) OR (is_hawk_indexed = 'Y')) AND ((product_type_id = 'FINISHED_GOOD') AND (domain_party_id = 'AAPA') AND (primary_product_category_id IS NOT NULL)))) AND ((sales_discontinuation_date IS NULL) OR (sales_discontinuation_date >= "+(new Date(new Timestamp(System.currentTimeMillis()).getTime()))+")) ORDER BY Products.aaia_brand_id;");
            } else {  //1235
                productFeatureAndAppls = queryData1("SELECT * FROM (SELECT P.PRODUCT_ID, P.SALES_DISCONTINUATION_DATE, P.ALLIANCE_PRODUCT_ID, P.IS_HAWK_INDEXED, P.HAWK_INDEXED_DATE, P.STATUS_ID, P.PRODUCT_TYPE_ID, P.DOMAIN_PARTY_ID, P.PRIMARY_PRODUCT_CATEGORY_ID, P.LAST_MODIFIED_DATE, P.PRODUCT_UPDATE_DATE, P.PART_NUMBER, P.AAIA_BRAND_ID, P.AAIA_SUB_BRAND_ID, P.NATIONAL_POPULARITY_CODE, P.STD_PART_NUMBER, PCA.ATTR_NAME, PCA.ATTR_VALUE, PCA.PRODUCT_CATEGORY_ID FROM PRODUCT P LEFT OUTER JOIN PRODUCT_CATEGORY_ATTRIBUTE PCA ON P.AAIA_BRAND_ID = PCA.PRODUCT_CATEGORY_ID LEFT OUTER JOIN PRODUCT_CATEGORY_ATTRIBUTE PCAS ON P.AAIA_SUB_BRAND_ID = PCAS.PRODUCT_CATEGORY_ID WHERE (((((PCAS.ATTR_NAME = 'HAWKSEARCH_ENABLED' OR PCA.ATTR_NAME = 'HAWKSEARCH_ENABLED') AND (PCAS.ATTR_VALUE = 'Y' OR PCA.ATTR_VALUE = 'Y') AND P.ALLIANCE_PRODUCT_ID IS NOT NULL AND P.DOMAIN_PARTY_ID = 'AAPA')))) ORDER BY P.PRODUCT_ID ASC, PCA.ATTR_NAME ASC, PCA.PRODUCT_CATEGORY_ID ASC) AS Products WHERE ((((status_id IS NOT NULL) OR  (status_id != 'PRODUCT_STS_7')  OR (status_id != 'PRODUCT_STS_8') OR (status_id != 'PRODUCT_STS_9')) OR (is_hawk_indexed = 'Y')) AND ((product_type_id = 'FINISHED_GOOD') AND (domain_party_id = 'AAPA') AND (primary_product_category_id IS NOT NULL))) AND ((sales_discontinuation_date IS NULL) OR (sales_discontinuation_date >= "+(new Date(new Timestamp(System.currentTimeMillis()).getTime()))+")) ORDER BY Products.aaia_brand_id;");
  
          }


            //getting result set


            itemInfoMap = new HashMap<String, Object>();
            if(productFeatureAndAppls != null) {
                while(productFeatureAndAppls.next()){
                    dataResourceId = null;
                    textDataProduct = null;
                    terminology = null;
                    brand = null;
                    electronicText = null;
                    textDataMarket = null;
                    jobberPrice = null;
                    quotePrice = null;
                    userPrice = null;
                    wdPrice = null;

                     product = null;
                     subBrand = null;
                     goodCore = null;
                     jobberCore=null;
                     wdCore=null;

                    System.out.println("Part Number | ProductId [" + productFeatureAndAppls.getString("part_number") + " | " + productFeatureAndAppls.getString("product_id") + "]=======1===count-==="+count++);
                    productId = productFeatureAndAppls.getString("product_id");
                    partNumber = productFeatureAndAppls.getString("part_number");

                    if(partNumber == null || partNumber.isEmpty()) {
                        System.out.println("Part Number is not found for the product : " + productId);
                        continue;
                    }
                    if(productFeatureAndAppls.getString("alliance_product_id") == null || productFeatureAndAppls.getString("alliance_product_id").isEmpty()) {
                        System.out.println("Alliance Product ID is not found for the product : " + productId);
                        continue;
                    }

                    itemInfoMap.put("unique_id", productFeatureAndAppls.getString("alliance_product_id"));
                    if (productFeatureAndAppls.getString("national_popularity_code") == null || productFeatureAndAppls.getString("national_popularity_code").isEmpty()) {
                        System.out.println("National Popularity Code is not found for the product : " + productId);
                    }
                    itemInfoMap.put("sort_default", productFeatureAndAppls.getString("national_popularity_code"));

                    //Code to generate terminology name.
                    name = "";

        
        textDataProduct = queryData("SELECT PC.PURCHASE_FROM_DATE, PC.PURCHASE_THRU_DATE, PC.USE_TIME_UOM_ID, PC.THRU_DATE, PC.USE_ROLE_TYPE_ID, PC.MODIFIED_DATE, PC.CONTENT_ID, PC.PRODUCT_ID, PC.SEQUENCE_NUM, PC.FROM_DATE, PC.USE_COUNT_LIMIT, PC.PRODUCT_CONTENT_TYPE_ID, PC.USE_TIME, CO.CHILD_LEAF_COUNT, CO.OWNER_CONTENT_ID, CO.INSTANCE_OF_CONTENT_ID, CO.DATA_RESOURCE_ID, CO.DATA_SOURCE_ID, CO.SERVICE_NAME, CO.CUSTOM_METHOD_ID, CO.STATUS_ID, CO.CHILD_BRANCH_COUNT, CO.PRIVILEGE_ENUM_ID, CO.TEMPLATE_DATA_RESOURCE_ID, CO.DESCRIPTION, CO.DECORATOR_CONTENT_ID, CO.CREATED_DATE, CO.MIME_TYPE_ID, CO.CONTENT_NAME, CO.CHARACTER_SET_ID, CO.LOCALE_STRING, CO.LAST_MODIFIED_BY_USER_LOGIN, CO.CONTENT_TYPE_ID, CO.LAST_MODIFIED_DATE, CO.CREATED_BY_USER_LOGIN, ET.TEXT_DATA, DR.DATA_RESOURCE_NAME, DR.DATA_TEMPLATE_TYPE_ID, DR.MIME_TYPE_ID, DR.IS_PUBLIC, DR.RELATED_DETAIL_ID, DR.DATA_RESOURCE_ID, DR.DATA_SOURCE_ID, DR.DATA_CATEGORY_ID, DR.STATUS_ID, DR.CHARACTER_SET_ID, DR.OBJECT_INFO, DR.LAST_MODIFIED_BY_USER_LOGIN, DR.LOCALE_STRING, DR.DATA_RESOURCE_TYPE_ID, DR.SURVEY_RESPONSE_ID, DR.LAST_MODIFIED_DATE, DR.SURVEY_ID, DR.CREATED_DATE, DR.CREATED_BY_USER_LOGIN FROM PRODUCT_CONTENT PC INNER JOIN CONTENT CO ON PC.CONTENT_ID = CO.CONTENT_ID INNER JOIN DATA_RESOURCE DR ON CO.DATA_RESOURCE_ID = DR.DATA_RESOURCE_ID INNER JOIN ELECTRONIC_TEXT ET ON DR.DATA_RESOURCE_ID = ET.DATA_RESOURCE_ID WHERE ((PC.PRODUCT_CONTENT_TYPE_ID = 'PRODUCT_NAME' AND PC.PRODUCT_ID = '"+ productId +"') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date))) ORDER BY PC.CONTENT_ID ASC, PC.PRODUCT_ID ASC, PC.FROM_DATE ASC, PC.PRODUCT_CONTENT_TYPE_ID ASC, DR.DATA_RESOURCE_ID ASC LIMIT 1;");
        if(textDataProduct != null && textDataProduct.next()){
                            name = textDataProduct.getString("text_data");
                            }else {
                                name = "        ";
                            }
                            textDataProduct.close();
    	
                            statement.close();

                    itemInfoMap.put("name", formatContentForCsv(name)); // providing tab space instead of checking name is empty or not


                    terminologyName = "";
                  
                terminology = queryData("SELECT * FROM product_category WHERE product_category_id='"+ productFeatureAndAppls.getString("primary_product_category_id")+"' LIMIT 1;");

if (terminology != null && terminology.next() && terminology.getString("description") != null) {
                        terminologyName = terminology.getString("description");
                    }
                    
                    terminology.close();
                    statement.close();
        
                    //Code to generate image url.
                    //Code to send the thumbnail associated with 400 asset width image to hawk.

                    imageUrl = getImageUrl();
                    itemInfoMap.put("image", imageUrl);

                    //Code to generate brand code and brand name.
                    brandName = "";
                    brandCode = null;
        
                    brand = queryData("SELECT * FROM product_category WHERE product_category_id='"+ productFeatureAndAppls.getString("aaia_brand_id") +"' LIMIT 1;");


                    if (brand != null && brand.next()) {
                        brandName = brand.getString("description");
                        brandCode = brand.getString("category_name");
                    }
                    brand.close();
                    statement.close();
                    //Code to generate sub brand code.
                    subBrandCode = "";
                    subBrand = queryData("SELECT * FROM product_category WHERE product_category_id='"+ productFeatureAndAppls.getString("aaia_sub_brand_id") +"' LIMIT 1;");                   

                    if (subBrand != null  && subBrand.next()) {
                        subBrandCode = subBrand.getString("category_name");
                    }
            subBrand.close();
            statement.close();
      
                    itemInfoMap.put("url_detail", "HARD_CODED_JSON_PRODUCT_INFO");

                    //Code to generate short description.
                    descriptionShort = new StringBuilder(brandName);
                    if (!(terminologyName.isEmpty())) {
                        descriptionShort = descriptionShort.append(" ").append(terminologyName);
                        itemInfoMap.put("description_short", formatContentForCsv(descriptionShort.toString()));
                    }

                    textDataMarket  = queryData("SELECT PC.PURCHASE_FROM_DATE, PC.PURCHASE_THRU_DATE, PC.USE_TIME_UOM_ID, PC.THRU_DATE, PC.USE_ROLE_TYPE_ID, PC.MODIFIED_DATE, PC.CONTENT_ID, PC.PRODUCT_ID, PC.SEQUENCE_NUM, PC.FROM_DATE, PC.USE_COUNT_LIMIT, PC.PRODUCT_CONTENT_TYPE_ID, PC.USE_TIME, CO.CHILD_LEAF_COUNT, CO.OWNER_CONTENT_ID, CO.INSTANCE_OF_CONTENT_ID, CO.DATA_RESOURCE_ID, CO.DATA_SOURCE_ID, CO.SERVICE_NAME, CO.CUSTOM_METHOD_ID, CO.STATUS_ID, CO.CHILD_BRANCH_COUNT, CO.PRIVILEGE_ENUM_ID, CO.TEMPLATE_DATA_RESOURCE_ID, CO.DESCRIPTION, CO.DECORATOR_CONTENT_ID, CO.CREATED_DATE, CO.MIME_TYPE_ID, CO.CONTENT_NAME, CO.CHARACTER_SET_ID, CO.LOCALE_STRING, CO.LAST_MODIFIED_BY_USER_LOGIN, CO.CONTENT_TYPE_ID, CO.LAST_MODIFIED_DATE, CO.CREATED_BY_USER_LOGIN, ET.TEXT_DATA, DR.DATA_RESOURCE_NAME, DR.DATA_TEMPLATE_TYPE_ID, DR.MIME_TYPE_ID, DR.IS_PUBLIC, DR.RELATED_DETAIL_ID, DR.DATA_RESOURCE_ID, DR.DATA_SOURCE_ID, DR.DATA_CATEGORY_ID, DR.STATUS_ID, DR.CHARACTER_SET_ID, DR.OBJECT_INFO, DR.LAST_MODIFIED_BY_USER_LOGIN, DR.LOCALE_STRING, DR.DATA_RESOURCE_TYPE_ID, DR.SURVEY_RESPONSE_ID, DR.LAST_MODIFIED_DATE, DR.SURVEY_ID, DR.CREATED_DATE, DR.CREATED_BY_USER_LOGIN FROM PRODUCT_CONTENT PC INNER JOIN CONTENT CO ON PC.CONTENT_ID = CO.CONTENT_ID INNER JOIN DATA_RESOURCE DR ON CO.DATA_RESOURCE_ID = DR.DATA_RESOURCE_ID INNER JOIN ELECTRONIC_TEXT ET ON DR.DATA_RESOURCE_ID = ET.DATA_RESOURCE_ID WHERE ((PC.PRODUCT_CONTENT_TYPE_ID = 'MARKETING_DESC' AND PC.PRODUCT_ID = '"+ productId +"') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date))) ORDER BY PC.CONTENT_ID ASC, PC.PRODUCT_ID ASC, PC.FROM_DATE ASC, PC.PRODUCT_CONTENT_TYPE_ID ASC, DR.DATA_RESOURCE_ID ASC LIMIT 1;");
      
                    if(textDataMarket.next()) {
                    marketDesc = textDataMarket.getString("text_data");
                    } else {
                        marketDesc = "        ";
                    }
                    textDataMarket.close();
                        statement.close();

                    descriptionLong = "";
                    //Code to generate long description.
                    if (!(marketDesc.isEmpty())) {
                        descriptionLong = marketDesc;
                    }

                    //Code to generate the sku.
                    sku = new StringBuilder();
                    if (productFeatureAndAppls.getString("std_part_number") != null && !(productFeatureAndAppls.getString("std_part_number").isEmpty())) {
                        sku = sku.append(productFeatureAndAppls.getString("std_part_number")).append("-");
                    } else {
                        sku = sku.append("-");
                    }
                    if (brandCode != null && !(brandCode.isEmpty())) {
                        sku = sku.append(brandCode).append("-");
                    } else {
                        sku = sku.append("-");
                    }
                    if (!(subBrandCode.isEmpty())) {
                        sku = sku.append(subBrandCode);
                    }

                    itemInfoMap.put("sku", sku);

                    if (!(descriptionLong.isEmpty())) {
                        itemInfoMap.put("description_long", formatContentForCsv(descriptionLong.toString()));
                    }

                    // Generate Retail price
                    retailPrice = getPrice(productId);
                    itemInfoMap.put("retailPrice", retailPrice);

                    bufferWrite.write(itemInfoMap.get("unique_id")+"    "+itemInfoMap.get("name")+ "    "+itemInfoMap.get("url_detail")+"    "+itemInfoMap.get("image")+"    " + itemInfoMap.get("retailPrice")+ "    " + itemInfoMap.get("price_sale")+"    "+ itemInfoMap.get("priceSpecial")+"    "+  itemInfoMap.get("group_id")+"    "+ itemInfoMap.get("description_short")+"    "+ itemInfoMap.get("description_long")+"    "+ itemInfoMap.get("sku")+"    "+ itemInfoMap.get("sort_default")+"    "+ itemInfoMap.get("sort_rating") +"    "+ itemInfoMap.get("item_operation") +"\n");
                    itemInfoMap.clear();
             
                    statement.close();
                }  // end while
                bufferWrite.close();
                productFeatureAndAppls.close();
            } else {
                System.out.println("Unable to generate the data feed. No Product(s) in the system have Alliance Product ID set.");
            }
            con.close();
            con2.close();
            createZip(zipName, fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
}

        
        public static void connectToDB() {
            try{
                //Connection 1 to mysql server used for main SQL Stream mode
                Class.forName("com.mysql.jdbc.Driver");
                con=DriverManager.getConnection(
                "jdbc:mysql://172.20.20.128/"+ DB_NAME +"?autoReconnect=true","root","123456");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public static void connectToDB2() {
            try{
                //Connection 2 to mysql server used for Inner SQL
                Class.forName("com.mysql.jdbc.Driver");
                con2=DriverManager.getConnection(
                "jdbc:mysql://172.20.20.128/"+ DB_NAME +"?autoReconnect=true","root","123456");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
	// For INNER Query 
        public static ResultSet queryData(String query) {
            try {
 		statement = con.prepareStatement(query, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY); 
                //statement.setFetchSize(Integer.MIN_VALUE);
                statement.executeQuery();
                return statement.getResultSet();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

            
	// For main Query in streaming mode
	public static ResultSet queryData1(String query) {
            try {
		statement = con2.prepareStatement(query, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(Integer.MIN_VALUE);
                statement.executeQuery();
                return statement.getResultSet();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
        
        public static void updateQuery(String query) {
            try {
                PreparedStatement st = con.prepareStatement(query);
                //st.executeUpdate();
                st.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public static void createZip(String zip, String file) {
            try {
                File fs = new File(zip + ".zip");
                FileOutputStream fos = new FileOutputStream(fs);
                ZipOutputStream zos = new ZipOutputStream(fos);
                int i = 0;

                    ZipEntry zipEntry = new ZipEntry(file + ".txt");
                    zos.putNextEntry(zipEntry);
                    i++;

                zos.close();
                fos.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

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

            public static String getImageUrl() {
                return "Image.url";
            }

            public static BigDecimal getPrice(String productId) {
                BigDecimal rPrice = BigDecimal.ZERO;
                try{
                // Retail Price
					ResultSet zapPrice = queryData("SELECT * FROM product_price WHERE (product_id='"+ productId +"') AND (product_price_type_id='LIST_PRICE') AND (price_code='ZAP') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date)) LIMIT 1;");
							if (zapPrice != null && zapPrice.next()) {
                        rPrice = zapPrice.getBigDecimal("price").multiply(new BigDecimal(2.5));
                                zapPrice.close();
                                statement.close();
                    } else {
                        ResultSet listPrice = queryData("SELECT * FROM product_price WHERE (product_id='"+ productId +"') AND (product_price_type_id='LIST_PRICE') AND (price_code='LST') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date)) LIMIT 1;");
						if (listPrice != null && listPrice.next() && listPrice.getBigDecimal("price") != null) {
                            rPrice = listPrice.getBigDecimal("price");
                            listPrice.close();
                            statement.close();
                        } else {
                            ResultSet jobberPrice = queryData("SELECT * FROM product_price WHERE (product_id='"+ productId +"') AND (product_price_type_id='JOBBER_PRICE') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date)) LIMIT 1;");
							if (jobberPrice != null && jobberPrice.next() && jobberPrice.getBigDecimal("price") != null) {
                                rPrice = jobberPrice.getBigDecimal("price").multiply(new BigDecimal(2));
                                jobberPrice.close();
                                statement.close();
                            } else {
                                ResultSet quotePrice = queryData("SELECT * FROM product_price WHERE (product_id='"+ productId +"') AND (product_price_type_id='QOT') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date)) LIMIT 1;");
								if (quotePrice != null && quotePrice.next() && quotePrice.getBigDecimal("price") != null) {
                                    rPrice = quotePrice.getBigDecimal("price").multiply(new BigDecimal(2.8));
                                    quotePrice.close();
                                    statement.close();
                                } else {
                                    ResultSet userPrice = queryData("SELECT * FROM product_price WHERE (product_id='"+ productId +"') AND (product_price_type_id='USR') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date)) LIMIT 1;");
									if (userPrice != null && userPrice.next() && userPrice.getBigDecimal("price") != null) {
                                        rPrice = userPrice.getBigDecimal("price").multiply(new BigDecimal(2.8));
                                        userPrice.close();
                                        statement.close();
                                    } else {
                                        ResultSet wdPrice = queryData("SELECT * FROM product_price WHERE (product_id='"+ productId +"') AND (product_price_type_id='WD1') AND (DATE(CURRENT_DATE()) BETWEEN DATE(from_date) AND DATE(thru_date)) LIMIT 1;");
										if (wdPrice != null && wdPrice.next() && wdPrice.getBigDecimal("price") != null) {
                                            rPrice = wdPrice.getBigDecimal("price").multiply(new BigDecimal(2.8));
                                            wdPrice.close();
                                            statement.close();
                                        } else {
                                            rPrice = new BigDecimal(1234.56);
										}	
									}
								}
								
							}
						}
					}
                    
            } catch(SQLException ex) {
                ex.printStackTrace();
            } catch(Exception ex) {
                ex.printStackTrace();
            }
            return rPrice;
        }
	}
