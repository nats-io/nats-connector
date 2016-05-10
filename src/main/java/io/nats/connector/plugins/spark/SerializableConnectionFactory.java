/**
 * 
 */
package io.nats.connector.plugins.spark;

import static io.nats.client.Constants.PROP_CLOSED_CB;
import static io.nats.client.Constants.PROP_CONNECTION_NAME;
import static io.nats.client.Constants.PROP_CONNECTION_TIMEOUT;
import static io.nats.client.Constants.PROP_DISCONNECTED_CB;
import static io.nats.client.Constants.PROP_EXCEPTION_HANDLER;
import static io.nats.client.Constants.PROP_HOST;
import static io.nats.client.Constants.PROP_MAX_PENDING_BYTES;
import static io.nats.client.Constants.PROP_MAX_PENDING_MSGS;
import static io.nats.client.Constants.PROP_MAX_PINGS;
import static io.nats.client.Constants.PROP_MAX_RECONNECT;
import static io.nats.client.Constants.PROP_NORANDOMIZE;
import static io.nats.client.Constants.PROP_PASSWORD;
import static io.nats.client.Constants.PROP_PEDANTIC;
import static io.nats.client.Constants.PROP_PING_INTERVAL;
import static io.nats.client.Constants.PROP_PORT;
import static io.nats.client.Constants.PROP_RECONNECTED_CB;
import static io.nats.client.Constants.PROP_RECONNECT_ALLOWED;
import static io.nats.client.Constants.PROP_RECONNECT_BUF_SIZE;
import static io.nats.client.Constants.PROP_RECONNECT_WAIT;
import static io.nats.client.Constants.PROP_SECURE;
import static io.nats.client.Constants.PROP_SERVERS;
import static io.nats.client.Constants.PROP_TLS_DEBUG;
import static io.nats.client.Constants.PROP_URL;
import static io.nats.client.Constants.PROP_USERNAME;
import static io.nats.client.Constants.PROP_VERBOSE;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Properties;

import io.nats.client.ClosedCallback;
import io.nats.client.ConnectionFactory;
import io.nats.client.DisconnectedCallback;
import io.nats.client.ExceptionHandler;
import io.nats.client.ReconnectedCallback;

/**
 * @author laugimethods
 *
 */
public class SerializableConnectionFactory extends ConnectionFactory implements Serializable {
	
	protected String _url;
	protected String[] _servers;
	protected Properties _props;

	/**
	 * 
	 */
	public SerializableConnectionFactory() {
		super();
	}

	/**
	 * @param props
	 */
	public SerializableConnectionFactory(Properties props) {
		super(props);
		this._props = props;
	}

	/**
	 * @param url
	 */
	public SerializableConnectionFactory(String url) {
		super(url);
		this._url = url;
	}

	/**
	 * @param servers
	 */
	public SerializableConnectionFactory(String[] servers) {
		super(servers);
		this._servers = servers;
	}

	/**
	 * @param cf
	 */
	public SerializableConnectionFactory(ConnectionFactory cf) {
		super(cf);
	}

	/**
	 * @param url
	 * @param servers
	 */
	public SerializableConnectionFactory(String url, String[] servers) {
		super(url, servers);
		this._url = url;
		this._servers = servers;
	}
	
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }
 
    private Object writeReplace() throws ObjectStreamException {
        return this;
    }
 
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		
        if (_props != null) {
	        // PROP_URL
	        if (_props.containsKey(PROP_URL)) {
	            this.setUrl(_props.getProperty(PROP_URL, DEFAULT_URL));
	        }
	        // PROP_HOST
	        if (_props.containsKey(PROP_HOST)) {
	            this.setHost(_props.getProperty(PROP_HOST, DEFAULT_HOST));
	        }
	        // PROP_PORT
	        if (_props.containsKey(PROP_PORT)) {
	            this.setPort(
	                    Integer.parseInt(_props.getProperty(PROP_PORT, Integer.toString(DEFAULT_PORT))));
	        }
	        // PROP_USERNAME
	        if (_props.containsKey(PROP_USERNAME)) {
	            this.setUsername(_props.getProperty(PROP_USERNAME, null));
	        }
	        // PROP_PASSWORD
	        if (_props.containsKey(PROP_PASSWORD)) {
	            this.setPassword(_props.getProperty(PROP_PASSWORD, null));
	        }
	        // PROP_SERVERS
	        if (_props.containsKey(PROP_SERVERS)) {
	            String str = _props.getProperty(PROP_SERVERS);
	            if (str.isEmpty()) {
	                throw new IllegalArgumentException(PROP_SERVERS + " cannot be empty");
	            } else {
	                String[] servers = str.trim().split(",\\s*");
	                this.setServers(servers);
	            }
	        }
	        // PROP_NORANDOMIZE
	        if (_props.containsKey(PROP_NORANDOMIZE)) {
	            this.setNoRandomize(Boolean.parseBoolean(_props.getProperty(PROP_NORANDOMIZE)));
	        }
	        // PROP_CONNECTION_NAME
	        if (_props.containsKey(PROP_CONNECTION_NAME)) {
	            this.setConnectionName(_props.getProperty(PROP_CONNECTION_NAME, null));
	        }
	        // PROP_VERBOSE
	        if (_props.containsKey(PROP_VERBOSE)) {
	            this.setVerbose(Boolean.parseBoolean(_props.getProperty(PROP_VERBOSE)));
	        }
	        // PROP_PEDANTIC
	        if (_props.containsKey(PROP_PEDANTIC)) {
	            this.setPedantic(Boolean.parseBoolean(_props.getProperty(PROP_PEDANTIC)));
	        }
	        // PROP_SECURE
	        if (_props.containsKey(PROP_SECURE)) {
	            this.setSecure(Boolean.parseBoolean(_props.getProperty(PROP_SECURE)));
	        }
	        // PROP_TLS_DEBUG
	        if (_props.containsKey(PROP_TLS_DEBUG)) {
	            this.setTlsDebug(Boolean.parseBoolean(_props.getProperty(PROP_TLS_DEBUG)));
	        }
	        // PROP_RECONNECT_ALLOWED
	        if (_props.containsKey(PROP_RECONNECT_ALLOWED)) {
	            this.setReconnectAllowed(Boolean.parseBoolean(
	                    _props.getProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(true))));
	        }
	        // PROP_MAX_RECONNECT
	        if (_props.containsKey(PROP_MAX_RECONNECT)) {
	            this.setMaxReconnect(Integer.parseInt(_props.getProperty(PROP_MAX_RECONNECT,
	                    Integer.toString(DEFAULT_MAX_RECONNECT))));
	        }
	        // PROP_RECONNECT_WAIT
	        if (_props.containsKey(PROP_RECONNECT_WAIT)) {
	            this.setReconnectWait(Integer.parseInt(_props.getProperty(PROP_RECONNECT_WAIT,
	                    Integer.toString(DEFAULT_RECONNECT_WAIT))));
	        }
	        // PROP_RECONNECT_BUF_SIZE
	        if (_props.containsKey(PROP_RECONNECT_BUF_SIZE)) {
	            this.setReconnectBufSize(Integer.parseInt(_props.getProperty(PROP_RECONNECT_BUF_SIZE,
	                    Integer.toString(DEFAULT_RECONNECT_BUF_SIZE))));
	        }
	        // PROP_CONNECTION_TIMEOUT
	        if (_props.containsKey(PROP_CONNECTION_TIMEOUT)) {
	            this.setConnectionTimeout(Integer.parseInt(
	                    _props.getProperty(PROP_CONNECTION_TIMEOUT, Integer.toString(DEFAULT_TIMEOUT))));
	        }
	        // PROP_PING_INTERVAL
	        if (_props.containsKey(PROP_PING_INTERVAL)) {
	            this.setPingInterval(Integer.parseInt(_props.getProperty(PROP_PING_INTERVAL,
	                    Integer.toString(DEFAULT_PING_INTERVAL))));
	        }
	        // PROP_MAX_PINGS
	        if (_props.containsKey(PROP_MAX_PINGS)) {
	            this.setMaxPingsOut(Integer.parseInt(
	                    _props.getProperty(PROP_MAX_PINGS, Integer.toString(DEFAULT_MAX_PINGS_OUT))));
	        }
	        // PROP_EXCEPTION_HANDLER
	        if (_props.containsKey(PROP_EXCEPTION_HANDLER)) {
	            Object instance = null;
	            try {
	                String str = _props.getProperty(PROP_EXCEPTION_HANDLER);
	                Class<?> clazz = Class.forName(str);
	                Constructor<?> constructor = clazz.getConstructor();
	                instance = constructor.newInstance();
	            } catch (Exception e) {
	                throw new IllegalArgumentException(e);
	            } finally {
	            }
	            this.setExceptionHandler((ExceptionHandler) instance);
	        }
	        // PROP_CLOSED_CB
	        if (_props.containsKey(PROP_CLOSED_CB)) {
	            Object instance = null;
	            try {
	                String str = _props.getProperty(PROP_CLOSED_CB);
	                Class<?> clazz = Class.forName(str);
	                Constructor<?> constructor = clazz.getConstructor();
	                instance = constructor.newInstance();
	            } catch (Exception e) {
	                throw new IllegalArgumentException(e);
	            }
	            this.setClosedCallback((ClosedCallback) instance);
	        }
	        // PROP_DISCONNECTED_CB
	        if (_props.containsKey(PROP_DISCONNECTED_CB)) {
	            Object instance = null;
	            try {
	                String str = _props.getProperty(PROP_DISCONNECTED_CB);
	                Class<?> clazz = Class.forName(str);
	                Constructor<?> constructor = clazz.getConstructor();
	                instance = constructor.newInstance();
	            } catch (Exception e) {
	                throw new IllegalArgumentException(e);
	            }
	            this.setDisconnectedCallback((DisconnectedCallback) instance);
	        }
	        // PROP_RECONNECTED_CB
	        if (_props.containsKey(PROP_RECONNECTED_CB)) {
	            Object instance = null;
	            try {
	                String str = _props.getProperty(PROP_RECONNECTED_CB);
	                Class<?> clazz = Class.forName(str);
	                Constructor<?> constructor = clazz.getConstructor();
	                instance = constructor.newInstance();
	            } catch (Exception e) {
	                throw new IllegalArgumentException(e);
	            }
	            this.setReconnectedCallback((ReconnectedCallback) instance);
	        }
	        // PROP_MAX_PENDING_MSGS
	        if (_props.containsKey(PROP_MAX_PENDING_MSGS)) {
	            this.setMaxPendingMsgs(Integer.parseInt(_props.getProperty(PROP_MAX_PENDING_MSGS,
	                    Integer.toString(DEFAULT_MAX_PENDING_MSGS))));
	        }
	        // PROP_MAX_PENDING_BYTES
	        if (_props.containsKey(PROP_MAX_PENDING_BYTES)) {
	            this.setMaxPendingBytes(Long.parseLong(_props.getProperty(PROP_MAX_PENDING_BYTES,
	                    Long.toString(DEFAULT_MAX_PENDING_BYTES))));
	        }
        } else if (_url != null && _url.contains(",")) {
            this.setServers(_url);
        } else {
            this.setUrl(_url);
            this.setServers(_servers);
        }		
	}
}
