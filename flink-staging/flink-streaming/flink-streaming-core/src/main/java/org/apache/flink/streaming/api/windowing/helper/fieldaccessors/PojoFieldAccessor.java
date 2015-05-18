/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.helper.fieldaccessors;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

public class PojoFieldAccessor<R, F> implements FieldAccessor<R, F>, Serializable {

	private static final long serialVersionUID = 1L;

	PojoComparator comparator;
	Field keyField;

	PojoFieldAccessor(String field, TypeInformation<R> type, ExecutionConfig config) {
		if (!(type instanceof CompositeType<?>)) {
			throw new IllegalArgumentException(
					"Key expressions are only supported on POJO types and Tuples. "
							+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
		}

		@SuppressWarnings("unchecked")
		CompositeType<R> cType = (CompositeType<R>) type;

		List<CompositeType.FlatFieldDescriptor> fieldDescriptors = cType.getFlatFields(field);

		int logicalKeyPosition = fieldDescriptors.get(0).getPosition();
		Class<?> keyClass = fieldDescriptors.get(0).getType().getTypeClass();

		if (cType instanceof PojoTypeInfo) {
			comparator = (PojoComparator<R>) cType.createComparator(
					new int[] { logicalKeyPosition }, new boolean[] { false }, 0, config);
		} else {
			throw new IllegalArgumentException(
					"Key expressions are only supported on POJO types. "
							+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
		}

		keyField = comparator.getKeyFields()[0];
	}

	@SuppressWarnings("unchecked")
	@Override
	public F get(R record) {
		return (F) comparator.accessField(keyField, record);
	}

	@Override
	public R set(R record, F fieldValue) {
		try {
			keyField.set(record, fieldValue);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Could not modify the specified field.", e);
		}
		return record;
	}
}
