# Schema proxy: Object that is used by PgQuery to find schema objects
#
# This is used by other functions in this library, for example PgQuery#rewrite!.
#
# Note: The returned schema objects should implement .fingerprint in order to be used by PgQuery#fingerprint.

class PgQuery
  class SchemaProxy
    def find_schema_table(table_name, schema_name = nil, catalog_name = nil)
      raise "Not implemented"
    end

    def find_schema_column(column_name, table_name = nil, schema_name = nil, catalog_name = nil)
      raise "Not implemented"
    end

    # Used for loading the schema and statistics into pg_simulator
    #
    # Data structure is the same as used by pganalyze-collector
    def full_schema
      raise "Not implemented"
    end
  end

  attr_accessor :schema_proxy
end
