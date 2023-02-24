package mini_jedis.v1;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class Jedis {
	
	private Socket socket;
	
	public Jedis(String host, int port) throws UnknownHostException, IOException {
		socket = new Socket(host, port);
        socket.setReuseAddress(true);
        socket.setKeepAlive(true); // Will monitor the TCP connection is valid
        socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to ensure timely delivery of data
        socket.setSoLinger(true, 0); // Control calls close () method, the underlying socket is closed immediately
	}
	
	public String set(String key, String value) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("*3").append("\r\n")
		.append("$3").append("\r\n")
		.append("SET").append("\r\n")
		.append("$").append(key.length()).append("\r\n")
		.append(key).append("\r\n")
		.append("$").append(value.length()).append("\r\n")
		.append(value).append("\r\n");
		socket.getOutputStream().write(sb.toString().getBytes());
		InputStream is = socket.getInputStream();
		byte[] b = new byte[1024];
		int len = is.read(b);
		String ret = new String(b, 0, len);
		return ret;
	}
	
	public String get(String key) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("*2").append("\r\n")
		.append("$3").append("\r\n")
		.append("GET").append("\r\n")
		.append("$").append(key.length()).append("\r\n")
		.append(key).append("\r\n");
		socket.getOutputStream().write(sb.toString().getBytes());
		InputStream is = socket.getInputStream();
		byte[] b = new byte[1024];
		int len = is.read(b);
		String ret = new String(b, 0, len);
		return ret;
	}
	
	public static void main(String args[]) throws UnknownHostException, IOException {
		Jedis jedis = new Jedis("localhost", 6379);
		System.out.print(jedis.set("key3", "value3"));
		System.out.print(jedis.get("key3"));
	}

}
