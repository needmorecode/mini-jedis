package mini_jedis.v4;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;

import mini_jedis.v4.exception.JedisConnectionException;


public class Jedis implements Closeable {

	private String host;
	
	private int port;
	
	private Socket socket;

	private RedisOutputStream os;

	private RedisInputStream is;
	
	private boolean broken = false;
	
	private JedisPool dataSource = null;
	
	public Jedis(String host, int port) throws UnknownHostException, IOException {
		this.host = host;
		this.port = port;
		connect();
	}

	/**
	 * 响应示例：
	 * $5\r\n
	 * hello\r\n
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public String get(String key) {
		CommandArguments args = new CommandArguments();
		args.add(Command.GET.name());
		args.add(key);
		String resp = (String)this.executeCommand(args);
		return resp;
	}

	
	/**
	 * 
	 * 响应示例：
	 * +OK\r\n
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public String set(String key, String value) throws IOException {
		CommandArguments args = new CommandArguments();
		args.add(Command.SET.name());
		args.add(key);
		args.add(value);
		String resp = (String)this.executeCommand(args);
		return resp;
	}

	/**
	 * 响应示例：
	 * :3\r\n
	 * 
	 * @param key
	 * @param values
	 * @return
	 * @throws IOException
	 */
	public long rpush(String key, String... values) throws IOException {
		CommandArguments args = new CommandArguments();
		args.add(Command.RPUSH.name());
		args.add(key);
		args.addObjects(values);
		Long resp = (Long)this.executeCommand(args);
		return resp;
	}

	/**
	 * 响应示例：
	 * *2\r\n
	 * $5\r\n
	 * hello\r\n
	 * $5\r\n
	 * world\r\n
	 * 
	 * @param key
	 * @param start
	 * @param stop
	 * @return
	 * @throws IOException
	 */
	public List<String> lrange(final String key, final long start, final long stop) throws IOException {
		CommandArguments args = new CommandArguments();
		args.add(Command.LRANGE.name());
		args.add(key);
		args.add(start);
		args.add(stop);
		List<String> resp = (List<String>)this.executeCommand(args);
		return resp;
	}

	public void sendCommand(CommandArguments args) {
		try {
			ensureConnect();
			sendCommand0(args);
		} catch (JedisConnectionException e) {
			broken = true;
			throw e;
		}
	}
	
	private void sendCommand0(CommandArguments args) {
		try {
			os.write(Protocol.ASTERISK);
			os.writeIntCrLf(args.size());
			for (Object arg : args) {
				os.write(Protocol.DOLLAR);
				byte[] argBytes = String.valueOf(arg).getBytes();
				os.writeIntCrLf(argBytes.length);
				os.write(argBytes);
				os.writeCrLf();
			}
		} catch (IOException e) {
			throw new JedisConnectionException(e);
		}
	}

	public Object processReply() {
		if (broken) {
			throw new JedisConnectionException("Attempting to read from a broken connection");
		}
		
		try {
			final byte b = is.readByte();
			switch (b) {
			case Protocol.PLUS:
				return processStatusCodeReply();
			case Protocol.DOLLAR:
				return processBulkReply();
			case Protocol.COLON:
				return processInteger();
			case Protocol.ASTERISK:
				return processMultiBulkReply();
			default:
				throw new JedisConnectionException("can't parse reply");
			}
		} catch (JedisConnectionException e) {
			broken = true;
			throw e;
		}
	}

	private String processBulkReply() {
		final int len = is.readIntCrLf();
		if (len == -1) {
			return null;
		}

		final byte[] read = new byte[len];
		int offset = 0;
		while (offset < len) {
			final int size = is.read(read, offset, (len - offset));
			if (size == -1) {
				throw new JedisConnectionException("The server has closed the connection.");
			}
			offset += size;
		}

		// read 2 more bytes for the command delimiter
		is.readByte();
		is.readByte();

		return new String(read);
	}

	private String processStatusCodeReply() {
		return is.readLine();
	}

	private Long processInteger() {
		return is.readLongCrLf();
	}

	private List<Object> processMultiBulkReply() {
		final int num = is.readIntCrLf();
		if (num == -1) {
			return null;
		}
		final List<Object> ret = new ArrayList<>(num);
		for (int i = 0; i < num; i++) {
			ret.add(processReply());
		}
		return ret;
	}
	
	public Object executeCommand(CommandArguments args) {
		sendCommand(args);
		flush();
		Object reply = processReply();
		return reply;
	}
	
	public void flush() {
		try {
			os.flushBuffer();
		} catch (IOException e) {
			broken = true;
			throw new JedisConnectionException(e);
		}
	}
	
	public void ensureConnect() {
		if (!isConnected()) {
			connect();
		}
	}
	
	public boolean isConnected() {
	    return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
	            && !socket.isInputShutdown() && !socket.isOutputShutdown();
	}
	
	public void connect(){
		try {
			socket = new Socket();
			socket.setReuseAddress(true);
			socket.setKeepAlive(true); // Will monitor the TCP connection is valid
			socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to ensure timely delivery of data
			socket.setSoLinger(true, 0); // Control calls close () method, the underlying socket is closed immediately
			socket.connect(new InetSocketAddress(host, port), 2000);
			socket.setSoTimeout(2000);
			os = new RedisOutputStream(socket.getOutputStream());
			is = new RedisInputStream(socket.getInputStream());
		} catch (Exception e) {
			broken = true;
			throw new JedisConnectionException(e);
		} finally {
			if (broken) {
				closeSocket();
			}
		}
	}
	
	@Override
	public void close() {
		if (dataSource != null) {
			JedisPool pool = dataSource;
			dataSource = null;
			if (broken) {
				pool.returnBrokenResource(this);
			} else {
				pool.returnResource(this);
			}
		} else {
			closeConnection();
		}
		
	}
	
	private void closeSocket() {
		if (socket != null) {
			try {
				socket.close();
			} catch (Exception ignore) {
				// ignore
			}
		}
	}
	
	private void closeConnection() {
		if (isConnected()) {
			try {
				os.flushBuffer();
				socket.close();
			} catch (IOException e) {
				broken = true;
				throw new JedisConnectionException(e);
			} finally {
				closeSocket();
			}
		}
	}
	
	public void setDataSource(JedisPool jedisPool) {
		this.dataSource = jedisPool;
	}

	public static void main(String args[]) throws UnknownHostException, IOException {
		try (Jedis jedis = new Jedis("localhost", 6379)) {
			System.out.println(jedis.set("key1", "value1"));
			System.out.println(jedis.get("key1"));
			System.out.println(jedis.rpush("lkey1", "lvalue1", "lvalue2", "lvalue3"));
			System.out.println(jedis.lrange("lkey1", 1, 2));
		}
	}



}
