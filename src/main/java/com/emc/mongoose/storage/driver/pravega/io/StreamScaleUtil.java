package com.emc.mongoose.storage.driver.pravega.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.apache.logging.log4j.Level;

public interface StreamScaleUtil {

  static void scaleToFixedSegmentCount(
      final Controller controller,
      final int timeoutMillis,
      final String scopeName,
      final String streamName,
      final ScalingPolicy scalingPolicy) {
    val segments = controller.getCurrentSegments(scopeName, streamName).join();
    val segmentCount = segments.getSegments().size();
    val targetSegmentCount = scalingPolicy.getMinNumSegments();
    if (segmentCount == targetSegmentCount) {
      Loggers.MSG.info(
          "Stream \"{}/{}\" current segment count already equals the requested segment count ({}), will not "
              + "perform the scaling",
          scopeName,
          streamName,
          segmentCount);
    } else {
      val streamConfig = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
      try {
        val updateStreamFuture = controller.updateStream(scopeName, streamName, streamConfig);
        if (updateStreamFuture.get(timeoutMillis, TimeUnit.MILLISECONDS)) {
          Loggers.MSG.info(
              "Stream \"{}/{}\" has been updated w/ the config {}",
              scopeName,
              streamName,
              streamConfig);
          val stream = new StreamImpl(scopeName, streamName);
          val keyRangeChunk = 1.0 / segmentCount;
          val keyRanges =
              IntStream.range(0, segmentCount)
                  .boxed()
                  .collect(Collectors.toMap(x -> x * keyRangeChunk, x -> (x + 1) * keyRangeChunk));
          val segmentList =
              segments.getSegments().stream()
                  .map(Segment::getSegmentId)
                  .collect(Collectors.toList());
          if (controller
              .startScale(stream, segmentList, keyRanges)
              .get(timeoutMillis, TimeUnit.MILLISECONDS)) {
            Loggers.MSG.info(
                "Stream \"{}/{}\" has been scaled to the new segment count {}",
                scopeName,
                streamName,
                targetSegmentCount);
          } else {
            Loggers.ERR.warn(
                "Failed to scale the stream \"{}/{}\" to the new segment count {}",
                scopeName,
                streamName,
                targetSegmentCount);
          }
        } else {
          Loggers.ERR.warn(
              "Failed to update the stream \"{}/{}\" w/ the config: {}",
              scopeName,
              streamName,
              streamConfig);
        }
      } catch (final InterruptedException e) {
        throwUnchecked(e);
      } catch (final ExecutionException | TimeoutException e) {
        LogUtil.exception(
            Level.WARN,
            e,
            "Failed to update the stream \"{}/{}\" w/ the config: {}",
            scopeName,
            streamName,
            streamConfig);
      }
    }
  }
}
