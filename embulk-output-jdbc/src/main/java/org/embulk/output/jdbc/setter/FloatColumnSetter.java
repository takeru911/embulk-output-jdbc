package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class FloatColumnSetter
        extends ColumnSetter
{
    public FloatColumnSetter(BatchInsert batch, JdbcColumn column,
            DefaultValueSetter defaultValue)
    {
        super(batch, column, defaultValue);
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setFloat(v ? (float) 1.0 : (float) 0.0);
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setFloat((float) v);
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        batch.setFloat((float) v);
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        float fv;
        try {
            fv = Float.parseFloat(v);
        } catch (NumberFormatException e) {
            defaultValue.setFloat();
            return;
        }
        batch.setFloat(fv);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        defaultValue.setFloat();
    }
}
