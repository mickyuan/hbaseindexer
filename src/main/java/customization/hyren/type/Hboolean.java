package customization.hyren.type;

import com.google.common.collect.ImmutableList;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;

public class Hboolean implements ByteArrayValueMapper {

    private static Log log = LogFactory.getLog(Hboolean.class);
    /**
     * Map a byte array to a collection of values. The returned collection can be empty.
     * <p>
     * If a value cannot be mapped as requested, it should log the error and return an empty collection.
     *
     * @param input byte array to be mapped
     * @return mapped values
     */
    @Override
    public Collection<? extends Object> map(byte[] input) {
        try {
            return ImmutableList.of(mapInternal(Bytes.toString(input)));
        } catch (IllegalArgumentException e) {
            log.warn(
                    String.format("Error mapping byte value %s to %s", Bytes.toStringBinary(input),
                            boolean.class.getName()), e);
            return ImmutableList.of();
        }
    }

    private boolean mapInternal(String toString) {
        return Boolean.parseBoolean(toString);
    }
}
