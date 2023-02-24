package mini_jedis.v2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CommandArguments implements Iterable<String>{
	
	private List<String> args = new ArrayList<>();
	
	public void add(String arg) {
		args.add(arg);
	}

	public String getCommand() {
		return args.get(0);
	}

	@Override
	public Iterator<String> iterator() {
		return args.iterator();
	}
	
	public int size() {
		return args.size();
	}
	


}
