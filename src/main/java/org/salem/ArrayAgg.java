package org.salem;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;

import javax.inject.Inject;

// If dataset is too large, need : ALTER SESSION SET `planner.enable_hashagg` = false
public class ArrayAgg {

// STRING NULLABLE //	
	@FunctionTemplate(name = "array_agg", scope = FunctionScope.POINT_AGGREGATE, nulls = NullHandling.INTERNAL)
	public static class NullableVarChar_ArrayAgg implements DrillAggFunc {
		@Param
		VarCharHolder input;
		@Workspace
		ObjectHolder agg;
		@Output
		org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter out;
		@Inject
		DrillBuf buffer;

		@Override
		public void setup() {
			agg = new ObjectHolder();
		}

		@Override
		public void reset() {
			agg = new ObjectHolder();
		}

		@Override
		public void add() {
			org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter;
			if (agg.obj == null) {
				agg.obj = out.rootAsList();
			}

			org.apache.drill.exec.expr.holders.VarCharHolder rowHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();
			byte[] inputBytes = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers
					.getStringFromVarCharHolder(input).getBytes(com.google.common.base.Charsets.UTF_8);
			buffer.reallocIfNeeded(inputBytes.length);
			buffer.setBytes(0, inputBytes);
			rowHolder.start = 0;
			rowHolder.end = inputBytes.length;
			rowHolder.buffer = buffer;

			listWriter = (org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter) agg.obj;
			listWriter.varChar().write(rowHolder);
		}

		@Override
		public void output() {
			((org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter) agg.obj).endList();
		}
	}

// INTEGER NULLABLE //
	@FunctionTemplate(name = "array_agg", scope = FunctionScope.POINT_AGGREGATE, nulls = NullHandling.INTERNAL)
	public static class NullableInt_ArrayAgg implements DrillAggFunc {
		@Param
		IntHolder input;
		@Workspace
		ObjectHolder agg;
		@Output
		org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter out;
		@Inject
		DrillBuf buffer;

		@Override
		public void setup() {

			agg = new ObjectHolder();
		}

		@Override
		public void reset() {
			agg = new ObjectHolder();
		}

		@Override
		public void add() {
			org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter listWriter;
			if (agg.obj == null) {
				agg.obj = out.rootAsList();
			}


			listWriter = (org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter) agg.obj;
			listWriter.integer().writeInt(input.value);
		}

		@Override
		public void output() {
			((org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter) agg.obj).endList();
		}
	}

// ...
}