package com.emc.mongoose.storage.driver.pravega;

import com.github.akurilov.confuse.Config;
import io.pravega.client.stream.ScalingPolicy;

import lombok.val;

public interface PravegaScalingConfig {

	enum Type {
		FIXED, EVENT_RATE, KBYTE_RATE,
	}

	/**
	 Convert the Mongoose's scaling configuration to the Pravega's scaling policy instance
	 @param scalingConfig Mongoose's scaling config
	 @return Pravega's scaling policy instance
	 @throws IllegalArgumentException if the config contains unexpected scaling type string either scaling policy
	 	arguments don't meet its own preconditions
	 @throws NullPointerException if the config is null either scaling type string is null
	 */
	static ScalingPolicy scalingPolicy(final Config scalingConfig)
					throws IllegalArgumentException, NullPointerException {
		val scalingType = Type.valueOf(scalingConfig.stringVal("type").toUpperCase());
		val rate = scalingConfig.intVal("rate");
		val factor = scalingConfig.intVal("factor");
		val segmentCount = scalingConfig.intVal("segments");
		switch (scalingType) {
		case FIXED:
			return ScalingPolicy.fixed(segmentCount);
		case EVENT_RATE:
			return ScalingPolicy.byEventRate(rate, factor, segmentCount);
		case KBYTE_RATE:
			return ScalingPolicy.byDataRate(rate, factor, segmentCount);
		default:
			throw new AssertionError();
		}
	}
}
