package mini_jedis.v4;

import java.io.IOException;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class JedisFactory implements PooledObjectFactory<Jedis> {

	private String host;
	
	private int port;
	
	public JedisFactory(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	@Override
	public void activateObject(PooledObject<Jedis> p) throws Exception {
	}

	@Override
	public void destroyObject(PooledObject<Jedis> p) throws Exception {
		Jedis jedis = p.getObject();
		if (jedis.isConnected()) {
			jedis.close();
		}
	}

	@Override
	public PooledObject<Jedis> makeObject() throws Exception {
	    Jedis jedis = null;
		try {
			jedis = new Jedis(host, port);
			jedis.connect();
			return new DefaultPooledObject<>(jedis);
	    } catch (Exception e) {
	    	if (e instanceof IOException) {
	    		if (jedis != null) {
	    			jedis.close();
	    		}
	    	}
	    	throw e;
	    }
	}

	@Override
	public void passivateObject(PooledObject<Jedis> p) throws Exception {
	}

	@Override
	public boolean validateObject(PooledObject<Jedis> p) {
		Jedis jedis = p.getObject();
		try {
			return jedis.isConnected();
		} catch (Exception e) {
			return false;
		}
	}

}
