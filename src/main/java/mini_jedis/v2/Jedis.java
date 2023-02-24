package mini_jedis.v2;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Jedis {

	private Socket socket;

	private RedisOutputStream os;

	private RedisInputStream is;

	public Jedis(String host, int port) throws UnknownHostException, IOException {
		socket = new Socket(host, port);
		socket.setKeepAlive(true); // Will monitor the TCP connection is valid
		socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to ensure timely delivery of data
		socket.setSoLinger(true, 0); // Control calls close () method, the underlying socket is closed immediately
		os = new RedisOutputStream(socket.getOutputStream());
		is = new RedisInputStream(socket.getInputStream());
	}

	public String get(String key) throws IOException {
		CommandArguments args = new CommandArguments();
		args.add(Command.GET.name());
		args.add(key);
		sendCommand(args);
		os.flushBuffer();
		String resp = processReply();
		return resp;
	}

	public String set(String key, String value) throws IOException {
		CommandArguments args = new CommandArguments();
		args.add(Command.SET.name());
		args.add(key);
		args.add(value);
		sendCommand(args);
		os.flushBuffer();
		String resp = processReply();
		return resp;
	}

	public void sendCommand(CommandArguments args) throws IOException {
		os.write(Protocol.ASTERISK);
		os.writeIntCrLf(args.size());
		for (String arg : args) {
			os.write(Protocol.DOLLAR);
			byte[] argBytes = arg.getBytes();
			os.writeIntCrLf(argBytes.length);
			os.write(argBytes);
			os.writeCrLf();
		}
	}

	public String processReply() throws IOException {
		final byte b = is.readByte();
		switch (b) {
		case Protocol.PLUS:
			return processStatusCodeReply();
		case Protocol.DOLLAR:
			return processBulkReply();
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

	public static void main(String args[]) throws UnknownHostException, IOException {
		Jedis jedis = new Jedis("localhost", 6379);
		System.out.println(jedis.set("key3", "value3"));
		System.out.println(jedis.get("key3"));
	}

}
