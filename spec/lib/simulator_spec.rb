require 'spec_helper'
require 'pg'
require 'json'
require 'pp'

def load_sample(file_basename)
  connection = PG.connect(dbname: 'postgres')
  schema = JSON.parse(File.read(File.expand_path("../fixtures/files/#{file_basename}.json", File.dirname(__FILE__))))
  @env = PgSimulator::Environment.new(connection, schema)
end

describe PgSimulator do
  after(:each) do
    @env.destroy if @env
  end
  
  it "should work for a basic simulation" do
    load_sample("schema_migrations")
    plan = @env.explain("SELECT * FROM schema_migrations WHERE version = ((SELECT null::text)::text)")
    #pp plan
    plan[0]["Plan"].delete("Startup Cost")
    plan[0]["Plan"].delete("Total Cost")
    expect(plan).to eq [{"Plan"=>
     {"Node Type"=>"Seq Scan",
      "Relation Name"=>"schema_migrations",
      "Schema"=>"public",
      "Alias"=>"schema_migrations",
      "Plan Rows"=>1,
      "Plan Width"=>15,
      "Output"=>["version"],
      "Filter"=>"((schema_migrations.version)::text = $0)",
      "Plans"=>
       [{"Node Type"=>"Result",
         "Parent Relationship"=>"InitPlan",
         "Subplan Name"=>"InitPlan 1 (returns $0)",
         "Startup Cost"=>0.0,
         "Total Cost"=>0.01,
         "Plan Rows"=>1,
         "Plan Width"=>0,
         "Output"=>["NULL::text"]}]}}]
  end
  
  it "should allow us to disable seqscan" do
    load_sample("schema_migrations")
    plan = @env.explain("SELECT * FROM schema_migrations WHERE version = ((SELECT null::text)::text)",
                        {enable_seqscan: "off"})
    plan[0]["Plan"].delete("Startup Cost")
    plan[0]["Plan"].delete("Total Cost")
    expect(plan).to eq [{"Plan"=>
     {"Node Type"=>"Index Only Scan",
      "Scan Direction"=>"Forward",
      "Index Name"=>"unique_schema_migrations",
      "Relation Name"=>"schema_migrations",
      "Schema"=>"public",
      "Alias"=>"schema_migrations",
      "Plan Rows"=>1,
      "Plan Width"=>15,
      "Output"=>["version"],
      "Index Cond"=>"(schema_migrations.version = $0)",
      "Plans"=>
       [{"Node Type"=>"Result",
         "Parent Relationship"=>"InitPlan",
         "Subplan Name"=>"InitPlan 1 (returns $0)",
         "Startup Cost"=>0.0,
         "Total Cost"=>0.01,
         "Plan Rows"=>1,
         "Plan Width"=>0,
         "Output"=>["NULL::text"]}]}}]
  end
  
  it "should allow us to explain more complex queries" do
    load_sample("query_snapshot_hourlies")
    plan = @env.explain("SELECT query_id
                           FROM query_snapshot_hourlies
                          WHERE database_id = ((SELECT null::int)::int)
                                AND collected_at > NOW() - ((SELECT null::interval)::interval)
                                AND calls > ((SELECT null::bigint)::bigint)
                          GROUP BY query_id HAVING COUNT(query_id) > ((SELECT null::int)::int)",
                        {enable_seqscan: "off"})

    plan[0]["Plan"].delete("Startup Cost")
    plan[0]["Plan"].delete("Total Cost")
    plan[0]["Plan"]["Plans"].last.delete("Startup Cost")
    plan[0]["Plan"]["Plans"].last.delete("Total Cost")
    expect(plan).to eq [{"Plan"=>
     {"Node Type"=>"Aggregate",
      "Strategy"=>"Hashed",
      "Plan Rows"=>1, # FIXME: Actually 42
      "Plan Width"=>4,
      "Output"=>["query_id"],
      "Filter"=>"(count(query_snapshot_hourlies.query_id) > $3)",
      "Plans"=>
       [{"Node Type"=>"Result",
         "Parent Relationship"=>"InitPlan",
         "Subplan Name"=>"InitPlan 1 (returns $0)",
         "Startup Cost"=>0.0,
         "Total Cost"=>0.01,
         "Plan Rows"=>1,
         "Plan Width"=>0,
         "Output"=>["NULL::integer"]},
        {"Node Type"=>"Result",
         "Parent Relationship"=>"InitPlan",
         "Subplan Name"=>"InitPlan 2 (returns $1)",
         "Startup Cost"=>0.0,
         "Total Cost"=>0.01,
         "Plan Rows"=>1,
         "Plan Width"=>0,
         "Output"=>["NULL::interval"]},
        {"Node Type"=>"Result",
         "Parent Relationship"=>"InitPlan",
         "Subplan Name"=>"InitPlan 3 (returns $2)",
         "Startup Cost"=>0.0,
         "Total Cost"=>0.01,
         "Plan Rows"=>1,
         "Plan Width"=>0,
         "Output"=>["NULL::bigint"]},
        {"Node Type"=>"Result",
         "Parent Relationship"=>"InitPlan",
         "Subplan Name"=>"InitPlan 4 (returns $3)",
         "Startup Cost"=>0.0,
         "Total Cost"=>0.01,
         "Plan Rows"=>1,
         "Plan Width"=>0,
         "Output"=>["NULL::integer"]},
        {"Node Type"=>"Index Scan", # FIXME: Actually "Bitmap Heap Scan"
         "Parent Relationship"=>"Outer",
         "Scan Direction"=>"Forward",
         "Index Name"=>"query_snapshot_hourlies_unique",
         "Relation Name"=>"query_snapshot_hourlies",
         "Schema"=>"public",
         "Alias"=>"query_snapshot_hourlies",
         "Plan Rows"=>1, # FIXME: Actually 14862
         "Plan Width"=>4,
         "Output"=>
          ["query_id", "database_id", "collected_at", "calls", "total_time"],
         "Index Cond"=> # FIXME: Actually Recheck Cond
          "((query_snapshot_hourlies.database_id = $0) AND (query_snapshot_hourlies.collected_at > (now() - $1)))",
         "Filter"=>"(query_snapshot_hourlies.calls > $2)"}]}}]
         # FIXME: "Bitmap Index Scan" goes here with total cost 1934.41, plan rows 44585
  end
end