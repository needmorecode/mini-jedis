package mini_jedis.v4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CommandArguments implements Iterable<Object>{
	
	private List<Object> args = new ArrayList<>();
	
	public void add(Object arg) {
		args.add(arg);
	}
	
	public void addObjects(Object... args) {
		for (Object arg : args) {
			this.args.add(arg);
		}
	}

	public String getCommand() {
		return (String)args.get(0);
	}

	@Override
	public Iterator<Object> iterator() {
		return args.iterator();
	}
	
	public int size() {
		return args.size();
	}
	


}
