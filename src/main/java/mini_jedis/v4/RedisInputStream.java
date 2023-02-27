package mini_jedis.v4;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import mini_jedis.v4.exception.JedisConnectionException;

public class RedisInputStream extends FilterInputStream {

	protected final byte[] buf;

	// count: 当前读到的位置
	// limit: 缓冲区有用数据的边界
	protected int count, limit;

	private static final int BUFFER_SIZE = 8192;

	protected RedisInputStream(InputStream in) {
		super(in);
		buf = new byte[BUFFER_SIZE];
	}

	public byte readByte() {
		ensureFill();
		return buf[count++];
	}

	public String readLine() {
		final StringBuilder sb = new StringBuilder();
		while (true) {
			ensureFill();

			byte b = buf[count++];
			if (b == '\r') {
				ensureFill(); // Must be one more byte

				byte c = buf[count++];
				if (c == '\n') {
					break;
				}
				sb.append((char) b);
				sb.append((char) c);
			} else {
				sb.append((char) b);
			}
		}

		final String reply = sb.toString();
		if (reply.length() == 0) {
			throw new RuntimeException("The server has closed the connection.");
		}

		return reply;
	}

	public int readIntCrLf() {
		return (int) readLongCrLf();
	}

	public long readLongCrLf() {
		final byte[] buf = this.buf;
		ensureFill();
		long value = 0;
		while (true) {
			ensureFill();
			final int b = buf[count++];
			if (b == '\r') {
				ensureFill();
				if (buf[count++] != '\n') {
					throw new RuntimeException("Unexpected character!");
				}

				break;
			} else {
				value = value * 10 + b - '0';
			}
		}

		return value;
	}

	private void ensureFill() {
		try {
			if (count >= limit) {
				limit = in.read(buf);
				count = 0;
				if (limit == -1) {
					throw new JedisConnectionException("Unexpected end of stream.");
				}
			}
		} catch (IOException e) {
			throw new JedisConnectionException(e);
		}
	}

	@Override
	public int read(byte[] b, int off, int len) {
		ensureFill();
		final int length = Math.min(limit - count, len);
		System.arraycopy(buf, count, b, off, length);
		count += length;
		return length;
	}

}
