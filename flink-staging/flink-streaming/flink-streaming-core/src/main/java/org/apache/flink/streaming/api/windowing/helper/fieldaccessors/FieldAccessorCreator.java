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
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class FieldAccessorCreator {

	@SuppressWarnings("unchecked")
	public static <R, F> FieldAccessor<R, F> create(int pos, TypeInformation<R> typeInfo) {
		if (typeInfo.isTupleType()) {
			return new TupleFieldAccessor<R, F>(pos);
		} else if (typeInfo instanceof BasicArrayTypeInfo
				|| typeInfo instanceof PrimitiveArrayTypeInfo) {
			return new ArrayFieldAccessor<R, F>(pos);
		} else {
			return (FieldAccessor<R, F>) new SimpleFieldAccessor<R>();
		}
	}

	public static <R, F> FieldAccessor<R, F> create(String field, TypeInformation<R> typeInfo, ExecutionConfig config) {
		if(typeInfo.isTupleType()) {
			return new TupleFieldAccessor<R, F>(((TupleTypeInfo)typeInfo).getFieldIndex(field));
		} else {
			return new PojoFieldAccessor<R, F>(field, typeInfo, config);
		}
	}
}
