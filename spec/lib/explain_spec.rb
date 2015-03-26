require 'spec_helper'
require 'pp'

describe PgQuery, "explain" do
  it "should explain a simple query" do
    plan = explain_query("SELECT query_id
                            FROM query_snapshot_hourlies
                           WHERE database_id = ?
                                 AND collected_at > NOW() - INTERVAL ?
                                 AND calls > ?
                           GROUP BY query_id HAVING COUNT(query_id) > ?")

    expect(plan.raw_plan).to eq({
      "Node Type"=>"Aggregate",
      "Strategy"=>"Hashed",
      "Plan Rows"=>1,
      "Plan Width"=>4,
      "Output"=>["query_id"],
      "Filter"=>"(count(query_snapshot_hourlies.query_id) > $3)",
      "Group Key" => ["query_snapshot_hourlies.query_id"],
      "Plans"=>
       [{"Node Type"=>"Index Scan",
         "Parent Relationship"=>"Outer",
         "Scan Direction"=>"Forward",
         "Index Name"=>"query_snapshot_hourlies_unique",
         "Relation Name"=>"query_snapshot_hourlies",
         "Schema"=>"public",
         "Alias"=>"query_snapshot_hourlies",
         "Plan Rows"=>1,
         "Plan Width"=>4,
         "Output"=>
          ["query_id", "database_id", "collected_at", "calls", "total_time"],
         "Index Cond"=>
          "((query_snapshot_hourlies.database_id = $0) AND (query_snapshot_hourlies.collected_at > (now() - $1)))",
         "Filter"=>"(query_snapshot_hourlies.calls > $2)"}]})
  end

  it "should explain a query with substitution characters in the target list" do
    skip
  end
end
