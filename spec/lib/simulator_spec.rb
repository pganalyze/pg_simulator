require 'spec_helper'
require 'pg'
require 'json'
require 'pp'

describe PgSimulator do
  before(:each) do
    connection = PG.connect(dbname: 'pganalyze')
    schema = JSON.parse(File.read(File.expand_path('../fixtures/files/schema_migrations.json', File.dirname(__FILE__))))
    @env = PgSimulator::Environment.new(connection, schema)
  end
  after(:each) do
    @env.destroy if @env
  end
  
  it "should work for a basic simulation" do
    plan = @env.explain("SELECT * FROM schema_migrations WHERE version = ((SELECT null::text)::text)")
    #pp plan
    expect(plan).to eq [{"Plan"=>
     {"Node Type"=>"Seq Scan",
      "Relation Name"=>"schema_migrations",
      "Schema"=>"public",
      "Alias"=>"schema_migrations",
      "Startup Cost"=>0.01,
      "Total Cost"=>0.01, # FIXME: Actually 1.95
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
    plan = @env.explain("SELECT * FROM schema_migrations WHERE version = ((SELECT null::text)::text)",
                        {enable_seqscan: "off"})
    expect(plan).to eq [{"Plan"=>
     {"Node Type"=>"Index Only Scan",
      "Scan Direction"=>"Forward",
      "Index Name"=>"unique_schema_migrations",
      "Relation Name"=>"schema_migrations",
      "Schema"=>"public",
      "Alias"=>"schema_migrations",
      "Startup Cost"=>0.14, # FIXME: Actually 0.15
      "Total Cost"=>4.15, # FIXME: Actually 8.17
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
end