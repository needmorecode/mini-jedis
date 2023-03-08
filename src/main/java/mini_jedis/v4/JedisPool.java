package mini_jedis.v4;

import java.io.IOException;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import mini_jedis.v4.exception.JedisException;

public class JedisPool extends GenericObjectPool<Jedis> {

	public JedisPool(String host, int port) {
		super(new JedisFactory(host, port), new GenericObjectPoolConfig<Jedis>());
	}

	public Jedis getResource() throws Exception {
		Jedis jedis = super.borrowObject();
		jedis.setDataSource(this);
		return jedis;
	}

	public void returnResource(final Jedis resource) {
		if (resource != null) {
			try {
				super.returnObject(resource);
			} catch (Exception e) {
				this.returnBrokenResource(resource);
			}
		}
	}
	
	public void returnBrokenResource(final Jedis resource) {
		try {
			super.invalidateObject(resource);
		} catch (Exception e) {
			throw new JedisException(e);
		}
	}
	
	public static void main(String args[]) throws Exception {
		try (JedisPool pool = new JedisPool("localhost", 6379)) {
			try (Jedis jedis = pool.getResource()) {
				System.out.println(jedis.set("key1", "value1"));
				System.out.println(jedis.get("key1"));
				System.out.println(jedis.rpush("lkey1", "lvalue1", "lvalue2", "lvalue3"));
				System.out.println(jedis.lrange("lkey1", 1, 2));
			}
		}
	}
}
