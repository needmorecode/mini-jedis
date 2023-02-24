package mini_jedis.v3;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class RedisOutputStream extends FilterOutputStream {

	protected final byte[] buf;

	// 下一个写入的位置
	protected int count;

	private final static int BUFFER_SIZE = 8192;

	private final static int[] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999,
			Integer.MAX_VALUE };

	private final static byte[] DigitTens = { '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1', '1', '1', '1', '1',
			'1', '1', '1', '1', '1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '3', '3', '3', '3', '3', '3',
			'3', '3', '3', '3', '4', '4', '4', '4', '4', '4', '4', '4', '4', '4', '5', '5', '5', '5', '5', '5', '5',
			'5', '5', '5', '6', '6', '6', '6', '6', '6', '6', '6', '6', '6', '7', '7', '7', '7', '7', '7', '7', '7',
			'7', '7', '8', '8', '8', '8', '8', '8', '8', '8', '8', '8', '9', '9', '9', '9', '9', '9', '9', '9', '9',
			'9', };

	private final static byte[] DigitOnes = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9', };

	private final static byte[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
			'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

	public RedisOutputStream(OutputStream out) {
		super(out);
		buf = new byte[BUFFER_SIZE];
	}

	public void writeIntCrLf(int value) throws IOException {
		if (value < 0) {
			write((byte) '-');
			value = -value;
		}
		// 代码参考Integer.getChars()
		int size = 0;
		while (value > sizeTable[size])
			size++;

		size++;
		if (size >= buf.length - count) {
			flushBuffer();
		}

		int q, r;
		int charPos = count + size;

		while (value >= 65536) {
			q = value / 100;
			r = value - ((q << 6) + (q << 5) + (q << 2));
			value = q;
			buf[--charPos] = DigitOnes[r];
			buf[--charPos] = DigitTens[r];
		}

		for (;;) {
			q = (value * 52429) >>> (16 + 3);
			r = value - ((q << 3) + (q << 1));
			buf[--charPos] = digits[r];
			value = q;
			if (value == 0)
				break;
		}
		count += size;

		writeCrLf();
	}

	public void writeCrLf() throws IOException {
		if (2 >= buf.length - count) {
			flushBuffer();
		}

		buf[count++] = '\r';
		buf[count++] = '\n';
	}

	public void flushBuffer() throws IOException {
		if (count > 0) {
			out.write(buf, 0, count);
			count = 0;
		}
	}

	public void write(final byte b) throws IOException {
		if (count == buf.length) {
			flushBuffer();
		}
		buf[count++] = b;
	}

	public void write(final byte[] b, final int off, final int len) throws IOException {
		if (len >= buf.length) {
			flushBuffer();
			out.write(b, off, len);
		} else {
			if (len >= buf.length - count) {
				flushBuffer();
			}

			System.arraycopy(b, off, buf, count, len);
			count += len;
		}
	}

}
