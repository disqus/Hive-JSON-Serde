/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/
package org.openx.data.jsonserde;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import org.openx.data.jsonserde.json.JSONArray;
import org.apache.hadoop.io.Text;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringByteObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringDoubleObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringFloatObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringIntObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringLongObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringShortObjectInspector;

/**
 *
 * @author joey
 */
public class JsonStructObjectInspectorTest {

    public JsonStructObjectInspectorTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() {
    }

    /**
    * Test of getStructFieldData method, of class JsonStructObjectInspector.
    */
	@Test
	public void testGetStructFieldData() throws JSONException, SerDeException {
		System.out.println("getStructFieldData");

        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "_person_name,time,analytics_id,response_response_code,request_params_action,request_host,cookies_site_fb_connected,d_promoted_ids");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "string,string,string,string,string,string,string,string");
        JsonSerDe instance = new JsonSerDe();
        instance.initialize(conf, tbl);

		Writable w = new Text("{\"_person\":{\"name\":\"michael\"}, \"d\":{\"promoted_ids\": \"hi\", \"promoted\": \"michael\"}, \"time\":\"2011-08-17 11:16:21\",\"response\":{\"response_code\":200,\"content_length\":13795,\"redirected_to\":null,\"layout\":null},\"request\":{\"params\":{\"ver\":1337,\"hdnv\":\"true\",\"action\":\"geo\",\"do_not_redirect\":\"1\",\"controller\":\"cities\",\"ref\":\"fld\",\"was_geo\":\"true\",\"gwo_id\":\"1897392579\"},\"host\":\"www.site.com\",\"remote_addr\":\"192.168.0.100\",\"method\":\"GET\",\"request_uri\":\"/cities/geo?do_not_redirect=1&gwo_id=1897392579&hdnv=true&ref=fld&ver=1337&was_geo=true\",\"referrer\":null},\"analytics_id\":\"bae898bad7ff4a929859bfbdb51d1a0c\",\"cookies\":{\"_session\":\"8\",\"ref_code\":\"f\",\"preferred_city\":\"1\",\"site_fb_connected\":\"true\"}}");
		JSONObject result = (JSONObject) instance.deserialize(w);

        assertEquals("michael", result.get("_person_name"));
        assertEquals("2011-08-17 11:16:21", result.get("time"));
        assertEquals("bae898bad7ff4a929859bfbdb51d1a0c", result.get("analytics_id"));
        assertEquals("200", result.get("response_response_code"));
        assertEquals("geo", result.get("request_params_action"));
        assertEquals("www.site.com", result.get("request_host"));
        assertEquals("true", result.get("cookies_site_fb_connected"));
        assertEquals("hi", result.get("d_promoted_ids"));
	}

	/**
	* Test of getStructFieldsDataAsList method, of class JsonStructObjectInspector.
	*/
	// @Ignore
	// @Test
	// public void testGetStructFieldsDataAsList() {
	// 	System.out.println("getStructFieldsDataAsList");
	// 	Object o = null;
	// 	JsonStructObjectInspector instance = null;
	// 	List expResult = null;
	// 	List result = instance.getStructFieldsDataAsList(o);
	// 	assertEquals(expResult, result);
	// 	// TODO review the generated test code and remove the default call to fail.
	// 	fail("The test case is a prototype.");
	// }
}
