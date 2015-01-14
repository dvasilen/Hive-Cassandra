package org.apache.hadoop.hive.cassandra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import java.util.ArrayList;
import java.util.List;

public final class HiveCompatibilityUtils {
	
	private HiveCompatibilityUtils() {
	}
	
	public static List<Integer> getReadColumnIDs(Configuration conf) {
		String skips = conf.get("hive.io.file.readcolumn.ids", "");
		String[] list = StringUtils.split(skips);
		List<Integer> result = new ArrayList(list.length);
		for (String element : list) {
			Integer toAdd = Integer.valueOf(Integer.parseInt(element));
			if (!result.contains(toAdd)) {
				result.add(toAdd);
			}
		}
		return result;
	}
	
}