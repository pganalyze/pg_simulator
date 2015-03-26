module SpecSupport
  module ExplainQuery
    class FixtureSchemaProxy < PgQuery::SchemaProxy
      def initialize(file_basename)
        @full_schema = JSON.parse(File.read(File.expand_path("../fixtures/files/#{file_basename}.json", File.dirname(__FILE__))))
      end

      def full_schema
        @full_schema
      end
    end

    def explain_query(query)
      q = PgQuery.parse(query)
      q.schema_proxy = FixtureSchemaProxy.new('query_snapshot_hourlies')
      q.simulator_connection = PG.connect(dbname: 'postgres')
      plan = q.explain({enable_seqscan: "off"})
      plan.remove_costs!
      plan
    end
  end
end
