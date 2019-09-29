package customization.hyren.formatter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.uniquekey.BaseUniqueKeyFormatter;
import com.ngdata.hbaseindexer.uniquekey.UniqueTableKeyFormatter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 *  customize value in solr uniquekey like ${table name} + ${ rowkey }
 */
public class UniqueTableKeyFormatterImpl implements UniqueTableKeyFormatter {

    private static final HyphenEscapingUniqueKeyFormatter hyphenEscapingFormatter = new HyphenEscapingUniqueKeyFormatter();
    private static final Splitter SPLITTER = Splitter.onPattern("(?<!\\\\)-");
    private static final char SEPARATOR = '-';
    private static final Joiner JOINER = Joiner.on(SEPARATOR);
    private static final String delimiter = "@";

    public UniqueTableKeyFormatterImpl(){
        System.out.println("xxxxxxxxxxxx"+getClass().getClassLoader().hashCode());
    }


    /**
     * Extracts the table name from the formatted string
     *
     * @param value Formatted value containing the table name
     * @return Representation of the table name
     */
    @Override
    public byte[] unformatTable(String value) {
        return decodeFromString(value);
    }

    /**
     * Format a row key into a human-readable form.
     *
     * @param row       row key to be formatted
     * @param tableName
     */
    @Override
    public String formatRow(byte[] row, byte[] tableName) {
        return encodeAsString(tableName) + delimiter + encodeAsString(row);
    }

    /**
     * Format a column family value into a human-readable form.
     * <p>
     * Called as part of column-based mapping, {@link IndexerConf.MappingType#COLUMN}.
     *
     * @param family    family bytes to be formatted
     * @param tableName
     */
    @Override
    public String formatFamily(byte[] family, byte[] tableName) {
        Preconditions.checkNotNull(family, "family");
        return encodeAsString(family);
    }

    /**
     * Format a {@code KeyValue} into a human-readable form. Only the row, column family, and qualifier
     * of the {@code KeyValue} will be encoded.
     * <p>
     * Called in case of column-based mapping, {@link IndexerConf.MappingType#COLUMN}.
     *
     * @param keyValue  value to be formatted
     * @param tableName
     */
    @Override
    public String formatKeyValue(KeyValue keyValue, byte[] tableName) {
        return hyphenEscapingFormatter.formatKeyValue(keyValue);
    }

    /**
     * Format a row key into a human-readable form.
     *
     * @param row row key to be formatted
     */
    @Override
    public String formatRow(byte[] row) {
        Preconditions.checkNotNull(row, "row");
        return encodeAsString(row);
    }

    /**
     * Format a column family value into a human-readable form.
     * <p>
     * Called as part of column-based mapping, {@link IndexerConf.MappingType#COLUMN}.
     *
     * @param family family bytes to be formatted
     */
    @Override
    public String formatFamily(byte[] family) {
        Preconditions.checkNotNull(family, "family");
        return encodeAsString(family);
    }

    /**
     * Format a {@code KeyValue} into a human-readable form. Only the row, column family, and qualifier
     * of the {@code KeyValue} will be encoded.
     * <p>
     * Called in case of column-based mapping, {@link IndexerConf.MappingType#COLUMN}.
     *
     * @param keyValue value to be formatted
     */
    @Override
    public String formatKeyValue(KeyValue keyValue) {
        return JOINER.join(encodeAsString(keyValue.getRow()), encodeAsString(keyValue.getFamily()),
                encodeAsString(keyValue.getQualifier()));
    }

    /**
     * Perform the reverse formatting of a row key.
     *
     * @param keyString the formatted row key
     * @return the unformatted row key
     */
    @Override
    public byte[] unformatRow(String keyString) {
        return decodeFromString(keyString);
    }

    /**
     * Perform the reverse formatting of a column family value.
     *
     * @param familyString the formatted column family string
     * @return the unformatted column family value
     */
    @Override
    public byte[] unformatFamily(String familyString) {
        return decodeFromString(familyString);
    }

    /**
     * Perform the reverse formatting of a {@code KeyValue}.
     * <p>
     * The returned KeyValue will only have the row key, column family, and column qualifier filled in.
     *
     * @param keyValueString the formatted {@code KeyValue}
     * @return the unformatted {@code KeyValue}
     */
    @Override
    public KeyValue unformatKeyValue(String keyValueString) {
        List<String> parts = Lists.newArrayList(SPLITTER.split(keyValueString));
        if (parts.size() != 3) {
            throw new IllegalArgumentException("Value cannot be split into row, column family, qualifier: "
                    + keyValueString);
        }
        byte[] rowKey = decodeFromString(parts.get(0));
        byte[] columnFamily = decodeFromString(parts.get(1));
        byte[] columnQualifier = decodeFromString(parts.get(2));
        return new KeyValue(rowKey, columnFamily, columnQualifier);
    }


    private static class HyphenEscapingUniqueKeyFormatter extends BaseUniqueKeyFormatter {
        @Override
        protected String encodeAsString(byte[] bytes) {
            String encoded = Bytes.toString(bytes);
            if (encoded.indexOf('-') > -1) {
                encoded = encoded.replace("-", "\\-");
            }
            return encoded;
        }

        @Override
        protected byte[] decodeFromString(String value) {
            if (value.contains("\\-")) {
                value = value.replace("\\-", "-");
            }
            return Bytes.toBytes(value);
        }

    }

    private String encodeAsString(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    private byte[] decodeFromString(String value) {
        return Bytes.toBytes(value);
    }

}
