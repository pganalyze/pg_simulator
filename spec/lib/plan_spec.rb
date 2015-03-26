require 'spec_helper'

describe PgQuery::Plan do
  describe '#indices_per_column' do
    it "should work for a simple case" do
      plan = explain_query("SELECT * FROM query_snapshot_hourlies
                             WHERE database_id = ? AND total_time > ?")

      expect(plan.indices_per_column).to eq({
        ['query_snapshot_hourlies', 'database_id'] => ['query_snapshot_hourlies_unique'],
        ['query_snapshot_hourlies', 'total_time'] => [nil]})
    end

    it "should work for multi-column indices (1st, 2nd column)" do
      plan = explain_query("SELECT query_id
                              FROM query_snapshot_hourlies
                             WHERE database_id = ?
                                   AND collected_at > NOW() - INTERVAL ?
                                   AND calls > ?
                             GROUP BY query_id HAVING COUNT(query_id) > ?")

      expect(plan.indices_per_column).to eq({
        ['query_snapshot_hourlies', 'collected_at'] => ['query_snapshot_hourlies_unique'],
        ['query_snapshot_hourlies', 'database_id'] => ['query_snapshot_hourlies_unique'],
        ['query_snapshot_hourlies', 'calls'] => [nil]})
    end

    it "should work for multi-column indices (2nd column)" do
      plan = explain_query("SELECT query_id
                              FROM query_snapshot_hourlies
                             WHERE collected_at > NOW() - INTERVAL ?")

      expect(plan.indices_per_column).to eq({
        ['query_snapshot_hourlies', 'collected_at'] => ['query_snapshot_hourlies_unique']})

      # Note: This is actually a suboptimal case, and if
      # seqscan was on it'd probably not choose this.
    end

    it "should gives us multiple index states per column" do
      # Second query can't use the multi-column index here, since it doesn't
      # query the first column
      #
      # FIXME: We should test this behaviour by having two different indices,
      # idx1(a,b,c) AND idx2(b). Then check that columns[b] = [idx1, idx2]
      plan = explain_query("SELECT query_id
                              FROM query_snapshot_hourlies qsh1
                             WHERE database_id = ?
                                   AND collected_at = (SELECT collected_at
                                                         FROM query_snapshot_hourlies qsh2
                                                        WHERE query_id = ?)")

      #pp plan.raw_plan
      expect(plan.indices_per_column).to eq({
        ['query_snapshot_hourlies', 'collected_at'] => ['query_snapshot_hourlies_unique'],
        ['query_snapshot_hourlies', 'database_id'] => ['query_snapshot_hourlies_unique'],
        ['query_snapshot_hourlies', 'database_id'] => ['query_snapshot_hourlies_unique'],
        ["query_snapshot_hourlies", "query_id"] => ["query_snapshot_hourlies_unique"]})
    end
  end
end
