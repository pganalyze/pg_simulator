require 'spec_helper'

class SampleProxy < PgQuery::SchemaProxy
  def find_schema_table(table_name, schema_name = nil, catalog_name = nil)
    "TEST"
  end
end

describe PgQuery, "rewrite" do
  it "should resolve table references" do
    pending
    q = PgQuery.parse("SELECT 1 FROM y")
    q.schema_proxy = SampleProxy.new
    q.rewrite!
    expect(q.rewritten_parsetree).to eq [{"SELECT"=>
          {"distinctClause"=>nil,
           "intoClause"=>nil,
           "targetList"=>
            [{"RESTARGET"=>
               {"name"=>nil,
                "indirection"=>nil,
                "val"=>{"A_CONST"=>{"val"=>1, "location"=>7}},
                "location"=>7}}],
           "fromClause"=>
            [{"RANGEVAR"=>
               {"schemaname"=>nil,
                "relname"=>"y",
                "inhOpt"=>2,
                "relpersistence"=>"p",
                "alias"=>nil,
                "location"=>14}}],
           "whereClause"=>nil,
           "groupClause"=>nil,
           "havingClause"=>nil,
           "windowClause"=>nil,
           "valuesLists"=>nil,
           "sortClause"=>nil,
           "limitOffset"=>nil,
           "limitCount"=>nil,
           "lockingClause"=>nil,
           "withClause"=>nil,
           "op"=>0,
           "all"=>false,
           "larg"=>nil,
           "rarg"=>nil}}]
  end

  it "should resolve column names in target list" do
    pending
    q = PgQuery.parse("SELECT x FROM y")
    q.schema_proxy = SampleProxy.new
    q.rewrite!
    expect(q.rewritten_parsetree).to eq [{"SELECT"=>
          {"distinctClause"=>nil,
           "intoClause"=>nil,
           "targetList"=>
            [{"RESTARGET"=>
               {"name"=>nil,
                "indirection"=>nil,
                "val"=>{"VAR"=>{"schemaname"=>"public", "relname"=>"y", "colname"=>"x", "location"=>7}},
                "location"=>7}}],
           "fromClause"=>
            [{"RANGEVAR"=>
               {"schemaname"=>nil,
                "relname"=>"y",
                "inhOpt"=>2,
                "relpersistence"=>"p",
                "alias"=>nil,
                "location"=>14}}],
           "whereClause"=>nil,
           "groupClause"=>nil,
           "havingClause"=>nil,
           "windowClause"=>nil,
           "valuesLists"=>nil,
           "sortClause"=>nil,
           "limitOffset"=>nil,
           "limitCount"=>nil,
           "lockingClause"=>nil,
           "withClause"=>nil,
           "op"=>0,
           "all"=>false,
           "larg"=>nil,
           "rarg"=>nil}}]
  end

  it "should resolve aliases in simple cases" do
    pending
    # see transformColumnRef in postgres source
    q = PgQuery.parse("SELECT z.x FROM y z")
    q.schema_proxy = SampleProxy.new
    q.rewrite!
    expect(q.rewritten_parsetree).to eq [{"SELECT"=>
          {"distinctClause"=>nil,
           "intoClause"=>nil,
           "targetList"=>
            [{"RESTARGET"=>
               {"name"=>nil,
                "indirection"=>nil,
                "val"=>{"VAR"=>{"schemaname"=>"public", "relname"=>"y", "colname"=>"x", "location"=>7}},
                "location"=>7}}],
           "fromClause"=>
            [{"RANGEVAR"=>
               {"schemaname"=>nil,
                "relname"=>"y",
                "inhOpt"=>2,
                "relpersistence"=>"p",
                "alias"=>{"ALIAS"=>{"aliasname"=>"z", "colnames"=>nil}},
                "location"=>16}}],
           "whereClause"=>nil,
           "groupClause"=>nil,
           "havingClause"=>nil,
           "windowClause"=>nil,
           "valuesLists"=>nil,
           "sortClause"=>nil,
           "limitOffset"=>nil,
           "limitCount"=>nil,
           "lockingClause"=>nil,
           "withClause"=>nil,
           "op"=>0,
           "all"=>false,
           "larg"=>nil,
           "rarg"=>nil}}]
  end

  it "should resolve * in target list" do
    skip
    # see transformTargetList in postgres source
  end
end
