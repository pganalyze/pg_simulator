require 'pg_query/param_refs'

class PgQuery
  def explain(settings = {})
    raise "Error: Schema proxy not set" unless @schema_proxy
    raise "Error: Simulator connection not set" unless @simulator_connection

    @env = PgSimulator::Environment.new(@simulator_connection, @schema_proxy.full_schema)

    # Replace dummy paramrefs with actual working paramrefs,
    # plus actual typecasts for syntax like "INTERVAL ?"
    new_query = ""
    prevloc = 0
    refs = param_refs
    refs.each_with_index do |p, idx|
      new_query << @query[prevloc..p["location"]-1]
      new_query << "$%d" % [idx + 1]
      new_query << "::%s" % [p["typename"]] if p["typename"]
      prevloc = p["location"] + p["length"]
    end
    new_query << @query[prevloc..-1]

    # Infer all datatypes using the built-in Postgres logic for prepared statements
    @env.exec("PREPARE test AS " + new_query)
    datatypes = @env.exec("SELECT unnest(parameter_types) AS data_type
                             FROM pg_prepared_statements WHERE name = 'test'").values.flatten
    @env.exec("DEALLOCATE test")

    # Turn all param refs into unknown constant values that can be parsed by Postgres
    # The general form is "((SELECT null::paramtype)::paramtype)" (double parenthesis to not break ANY(...))
    #
    # This approach is taken from printRemotePlaceholder in contrib/postgres_fdw/deparse.c
    new_query = ""
    prevloc = 0
    refs.each_with_index do |p, idx|
      new_query << @query[prevloc..p["location"]-1]
      new_query << "((SELECT null::%s)::%s)" % [datatypes[idx], datatypes[idx]]
      prevloc = p["location"] + p["length"]
    end
    new_query << @query[prevloc..-1]

    # EXPLAIN
    plan = @env.explain(new_query, settings)

    plan = Plan.new(plan[0]['Plan'], self)

    # Clean up plan to remove dubious InitPlan nodes caused by our unknown constant values
    plan.delete_nodes! do |node|
      node["Node Type"] == "Result" &&
      node["Parent Relationship"] == "InitPlan" &&
      node["Subplan Name"][/^InitPlan \d+ \(returns \$\d+\)$/] &&
      node["Output"][0][/^NULL::/]
    end

    plan
  ensure
    @env.destroy if @env
  end
end
