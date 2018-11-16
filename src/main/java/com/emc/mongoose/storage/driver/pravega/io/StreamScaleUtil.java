package com.emc.mongoose.storage.driver.pravega.io;

import com.emc.mongoose.exception.InterruptRunException;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;

import lombok.val;
import org.apache.logging.log4j.Level;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface StreamScaleUtil {

	static void scaleToFixedSegmentCount(
		final Controller controller, final int timoutMillis, final String scopeName, final String streamName,
		final ScalingPolicy scalingPolicy
	) throws InterruptRunException {
		val segments = controller.getCurrentSegments(scopeName, streamName).join();
		val segmentCount = segments.getSegments().size();
		val targetSegmentCount = scalingPolicy.getMinNumSegments();
		if(segmentCount == targetSegmentCount) {
			Loggers.MSG.info(
				"Stream \"{}/{}\" current segment count already equals the requested segment count ({}), will not "
					+ "perform the scaling",
				scopeName, streamName, segmentCount
			);
		} else {
			val streamConfig = StreamConfiguration
				.builder()
				.scope(scopeName)
				.streamName(streamName)
				.scalingPolicy(scalingPolicy)
				.build();
			try {
				if(controller.updateStream(streamConfig).get(timoutMillis, TimeUnit.MILLISECONDS)) {
					Loggers.MSG.info(
						"Stream \"{}/{}\" has been updated w/ the config {}", scopeName, streamName, streamConfig
					);
					val stream = new StreamImpl(scopeName, streamName);
					val keyRangeChunk = 1.0 / segmentCount;
					val keyRanges = IntStream
						.range(0, segmentCount)
						.boxed()
						.collect(Collectors.toMap(x -> x * keyRangeChunk, x -> (x + 1) * keyRangeChunk));
					val segmentList = segments
						.getSegments()
						.stream()
						.map(Segment::getSegmentId)
						.collect(Collectors.toList());
					if(
						controller
							.startScale(stream, segmentList, keyRanges)
							.get(timoutMillis, TimeUnit.MILLISECONDS)
					) {
						Loggers.MSG.info(
							"Stream \"{}/{}\" has been scaled to the new segment count {}", scopeName, streamName,
							targetSegmentCount
						);
					} else {
						Loggers.ERR.warn(
							"Failed to scale the stream \"{}/{}\" to the new segment count {}", scopeName, streamName,
							targetSegmentCount
						);
					}
				} else {
					Loggers.ERR.warn(
						"Failed to update the stream \"{}/{}\" w/ the config: {}", scopeName, streamName, streamConfig
					);
				}
			} catch(final InterruptedException e) {
				throw new InterruptRunException(e);
			} catch(final ExecutionException | TimeoutException e) {
				LogUtil.exception(
					Level.WARN, e, "Failed to update the stream \"{}/{}\" w/ the config: {}", scopeName, streamName,
					streamConfig
				);
			}
		}
	}
}
