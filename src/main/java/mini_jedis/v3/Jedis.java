package mini_jedis.v3;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Jedis {

	private Socket socket;

	private RedisOutputStream os;

	private RedisInputStream is;

	public Jedis(String host, int port) throws UnknownHostException, IOException {
		socket = new Socket();
		socket.setReuseAddress(true);
		socket.setKeepAlive(true); // Will monitor the TCP connection is valid
		socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to ensure timely delivery of data
		socket.setSoLinger(true, 0); // Control calls close () method, the underlying socket is closed immediately
		socket.connect(new InetSocketAddress(host, port), 2000);
		socket.setSoTimeout(2000);
		os = new RedisOutputStream(socket.getOutputStream());
		is = new RedisInputStream(socket.getInputStream());
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
	public String get(String key) throws IOException {
		CommandArguments args = new CommandArguments();
		args.add(Command.GET.name());
		args.add(key);
		sendCommand(args);
		os.flushBuffer();
		String resp = (String) processReply();
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
		sendCommand(args);
		os.flushBuffer();
		String resp = (String) processReply();
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
		sendCommand(args);
		os.flushBuffer();
		Long resp = (Long) processReply();
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
		sendCommand(args);
		os.flushBuffer();
		List<String> resp = (List<String>) processReply();
		return resp;
	}

	public void sendCommand(CommandArguments args) throws IOException {
		os.write(Protocol.ASTERISK);
		os.writeIntCrLf(args.size());
		for (Object arg : args) {
			os.write(Protocol.DOLLAR);
			byte[] argBytes = String.valueOf(arg).getBytes();
			os.writeIntCrLf(argBytes.length);
			os.write(argBytes);
			os.writeCrLf();
		}
	}

	public Object processReply() throws IOException {
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
			throw new RuntimeException("can't parse reply");
		}
	}

	private String processBulkReply() throws IOException {
		final int len = is.readIntCrLf();
		if (len == -1) {
			return null;
		}

		final byte[] read = new byte[len];
		int offset = 0;
		while (offset < len) {
			final int size = is.read(read, offset, (len - offset));
			if (size == -1) {
				throw new RuntimeException("The server has closed the connection.");
			}
			offset += size;
		}

		// read 2 more bytes for the command delimiter
		is.readByte();
		is.readByte();

		return new String(read);
	}

	private String processStatusCodeReply() throws IOException {
		return is.readLine();
	}

	private Long processInteger() throws IOException {
		return is.readLongCrLf();
	}

	private List<Object> processMultiBulkReply() throws IOException {
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

	public static void main(String args[]) throws UnknownHostException, IOException {
		Jedis jedis = new Jedis("localhost", 6379);
		System.out.println(jedis.set("key1", "value1"));
		System.out.println(jedis.get("key1"));
		System.out.println(jedis.rpush("lkey1", "lvalue1", "lvalue2", "lvalue3"));
		System.out.println(jedis.lrange("lkey1", 1, 2));
	}

}
