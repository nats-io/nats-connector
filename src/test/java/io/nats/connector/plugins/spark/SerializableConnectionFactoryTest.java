/**
 * 
 */
package io.nats.connector.plugins.spark;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.nats.client.Constants.PROP_URL;

/**
 * @author laugimethods
 *
 */
public class SerializableConnectionFactoryTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSerializeURL() {
		String url = "nats://192.168.2.3:9090";
		SerializableConnectionFactory original = new SerializableConnectionFactory(url);
		SerializableConnectionFactory copy = SerializationUtils.clone(original);
		assertEquals(original.getUrlString(), copy.getUrlString());
	}

	@Test
	public void testSerializeProperties() {
		Properties prop = new Properties();
		prop.setProperty(PROP_URL, "nats://192.168.2.2:9091");
		SerializableConnectionFactory original = new SerializableConnectionFactory(prop);
		SerializableConnectionFactory copy = SerializationUtils.clone(original);
		assertEquals(original.getUrlString(), copy.getUrlString());
	}

}
