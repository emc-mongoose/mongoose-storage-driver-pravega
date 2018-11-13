package com.emc.mongoose.storage.driver.pravega.util;

import java.util.regex.Pattern;

/**
 Created by kurila on 26.01.17.
 */
public interface LogPatterns {

	// common
	Pattern ASCII_COLOR = Pattern.compile("\\u001B\\[?m?[\\u001B\\[0-9m;]*");

	Pattern DATE_TIME_ISO8601 = Pattern.compile(
			"(?<dateTime>[\\d]{4}-[\\d]{2}-[\\d]{2}T(?<time>[\\d]{2}:[\\d]{2}:[\\d]{2},[\\d]{3}))"
	);
	Pattern STD_OUT_LOG_LEVEL = Pattern.compile("(?<levelLog>[FEWIDT])");
	Pattern STD_OUT_CLASS_NAME = Pattern.compile("[A-Za-z]+[\\w]*");
	Pattern STD_OUT_THREAD_NAME = Pattern.compile("(?<nameThread>\\w[\\w\\s#.\\-<>]+\\w)");

	// metrics
	Pattern TYPE_LOAD = Pattern.compile(
			ASCII_COLOR.pattern() + "(?<typeLoad>[CREATDLUPNO]{4,6})" + ASCII_COLOR.pattern()
	);
	Pattern STD_OUT_CONCURRENCY = Pattern.compile(
			"(?<concurrency>[0-9]{1,7})x(?<driverCount>[0-9]{1,7})"
	);
	Pattern STD_OUT_CONCURRENCY_ACTUAL = Pattern.compile(
			"c=\\((?<concurrencyLastMean>[0-9.]+)\\)"
	);
	Pattern STD_OUT_ITEM_COUNTS = Pattern.compile(
			"n=\\((?<countSucc>\\d+)/\\\u001B*\\[*\\d*m*(?<countFail>\\d+)\\\u001B*\\[*\\d*m*\\)"
	);
	Pattern STD_OUT_METRICS_TIME = Pattern.compile(
			"t\\[s\\]=\\((?<jobDur>[0-9.]+[eE]?[0-9]{0,2})/(?<sumDur>[0-9.]+[eE]?[0-9]{0,2})\\)"
	);
	Pattern STD_OUT_METRICS_SIZE = Pattern.compile(
			"size=\\((?<size>[\\d.]+[KMGTPE]?B?)\\)"
	);
	Pattern STD_OUT_METRICS_TP = Pattern.compile(
			"TP\\[op/s\\]=\\((?<tpMean>[0-9.]+)/(?<tpLast>[0-9.]+)\\)"
	);
	Pattern STD_OUT_METRICS_BW = Pattern.compile(
			"BW\\[MB/s\\]=\\((?<bwMean>[0-9.]+)/(?<bwLast>[0-9.]+)\\)"
	);
	Pattern STD_OUT_METRICS_DUR = Pattern.compile(
			"dur\\[us\\]=\\((?<durAvg>[0-9]+)/(?<durMin>[0-9]+)/(?<durMax>[0-9]+)\\)"
	);
	Pattern STD_OUT_METRICS_LAT = Pattern.compile(
			"lat\\[us\\]=\\((?<latAvg>[0-9]+)/(?<latMin>[0-9]+)/(?<latMax>[0-9]+)\\)"
	);
	Pattern STD_OUT_METRICS_SINGLE = Pattern.compile(
			ASCII_COLOR.pattern() + DATE_TIME_ISO8601.pattern() + "\\s+" + STD_OUT_LOG_LEVEL.pattern() +
					"\\s+" + STD_OUT_CLASS_NAME.pattern() + "\\s" + STD_OUT_THREAD_NAME.pattern() + "\\s+" +
					TYPE_LOAD.pattern() + "-" + STD_OUT_CONCURRENCY.pattern() + ":\\s+" +
					STD_OUT_CONCURRENCY_ACTUAL.pattern() + ";\\s+" +
					STD_OUT_ITEM_COUNTS.pattern() + ";\\s+" + STD_OUT_METRICS_TIME.pattern() + ";\\s+" +
					STD_OUT_METRICS_SIZE.pattern() + ";\\s+" + STD_OUT_METRICS_TP.pattern() + ";\\s+" +
					STD_OUT_METRICS_BW.pattern() + ";\\s+" + STD_OUT_METRICS_DUR.pattern() + ";\\s+" +
					STD_OUT_METRICS_LAT.pattern()
	);
	Pattern OP_TYPE = Pattern.compile(ASCII_COLOR.pattern() + "(?<opType>[CREATDLUPNO]{4,6})\\s{0,2}"
			+ ASCII_COLOR.pattern());
	Pattern STD_OUT_METRICS_TABLE_ROW = Pattern.compile(
			"\\s*(?<stepName>[\\w\\-_.,;:~=+@]{1,10})\\|(?<timestamp>[\\d]{12})" +
					"\\|" + OP_TYPE.pattern() +
					"\\|\\s*(?<concurrencyCurr>[\\d]{1,10})" +
					"\\|(?<concurrencyLastMean>[\\d]+\\.?[\\d]*)\\s*" +
					"\\|\\s*(?<succCount>[\\d]{1,12})" +
					"\\|\\s*" + ASCII_COLOR.pattern() + "\\s*(?<failCount>[\\d]{1,6})" + ASCII_COLOR.pattern() +
					"\\|(?<stepTime>[\\d]+\\.?[\\d]*)\\s*" +
					"\\|(?<tp>[\\d]+\\.?[\\d]*)\\.?\\s*\\|(?<bw>[\\d]+\\.?[\\d]*)\\.?\\s*" +
					"\\|\\s*(?<lat>[\\d]{1,10})" +
					"\\|\\s*(?<dur>[\\d]{1,11})"
	);
	Pattern STD_OUT_METRICS_TABLE_ROW_FINAL = Pattern.compile(
			"\\*{120}\\R" + STD_OUT_METRICS_TABLE_ROW.pattern() + "\\R\\*{120}"
	);

	Pattern STD_OUT_LOAD_THRESHOLD_ENTRANCE = Pattern.compile(
			ASCII_COLOR.pattern() + DATE_TIME_ISO8601.pattern() + "\\s+" + STD_OUT_LOG_LEVEL.pattern() +
					"\\s+" + STD_OUT_CLASS_NAME.pattern() + "\\s+" + STD_OUT_THREAD_NAME.pattern() +
					"\\s+[\\-_#@\\(\\)\\w]+:\\s+the threshold of (?<threshold>[0-9]+) active tasks count is reached, " +
					"starting the additional metrics accounting"
	);

	Pattern STD_OUT_LOAD_THRESHOLD_EXIT = Pattern.compile(
			ASCII_COLOR.pattern() + DATE_TIME_ISO8601.pattern() + "\\s+" + STD_OUT_LOG_LEVEL.pattern() +
					"\\s+" + STD_OUT_CLASS_NAME.pattern() + "\\s+" + STD_OUT_THREAD_NAME.pattern() +
					"\\s+[\\-_#@\\(\\)\\w]+:\\s+the active tasks count is below the threshold of (?<threshold>[0-9]+), " +
					"stopping the additional metrics accounting"
	);
}
