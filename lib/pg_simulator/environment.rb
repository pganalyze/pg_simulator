require 'securerandom'

module PgSimulator
  class Environment
    # Note: Supplied connection needs to have CREATEDB rights
    def initialize(otherconn, schema)
      @dbname = "pg_simulator_%s" % SecureRandom.uuid.gsub('-', '_')
      @otherdb = {dbname: otherconn.db, host: otherconn.host, port: otherconn.port, user: otherconn.user}
      
      otherconn.exec_params("CREATE DATABASE %s" % otherconn.quote_ident(@dbname))
      @conn = PG.connect(@otherdb.merge(dbname: @dbname))
      
      create_schema!(schema)
    rescue => e
      destroy if @conn
      raise
    end
    
    def create_schema!(schema)
      schema.each do |table|
        colstats = {}
        columns = table['columns']
        columns.sort_by! {|c| c['position'] }
        columns.map! do |c|
          # Save for later
          colstats[c['position']] = c['stats']
          
          str = @conn.quote_ident(c['name'])
          str += " " + c['data_type']
          str += " NOT NULL" if c['not_null']
          str
        end
        @conn.exec "CREATE TABLE %s.%s (%s)" % [@conn.quote_ident(table['schema_name']), @conn.quote_ident(table['table_name']), columns.join(",")]
        
        table['indices'].each do |index|
          @conn.exec index['index_def']
        end
        
        relid = @conn.exec_params("SELECT oid
                                     FROM pg_catalog.pg_class
                                    WHERE relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1)
                                          AND relname = $2", [table['schema_name'], table['table_name']])[0]['oid']
        
        # relpages has to match the physical pages the planner can see (otherwise it scales reltuples)
        #table['stats']['relpages'] = 0
        # FIXME: Actually this is sub-optimal too. The planner still sees the zero pages, but has some heuristics to assume its not a seqscan. This skews the behaviour. We should try to get the planner to ignore the actual physical pages.
        # See http://www.postgresql.org/docs/9.3/static/row-estimation-examples.html
        
        @conn.exec_params "UPDATE pg_catalog.pg_class
                              SET relpages = $1, reltuples = $2, relallvisible = $3
                            WHERE oid = $4",
                          [table['stats']['relpages'], table['stats']['reltuples'],
                           table['stats']['relallvisible'], relid]
        
        colstats.each do |position, stats|
          stats['stanumbers2'] = stats['stanumbers2'].to_s.gsub('[', '{').gsub(']', '}')
          stavalues1 = stats.delete('stavalues1')
          @conn.exec_params("INSERT INTO pg_catalog.pg_statistic
                                         (starelid, staattnum, %s, stavalues1)
                                  VALUES (%s, array_in($26, 25, -1))" % [stats.keys.join(","), (1..(stats.size + 2)).map {|i| "$%d" % i }.join(",")],
                            [relid, position] + stats.values + [stavalues1])
        end
      end
    end
    
    def explain(query, settings = {})
      res = nil
      # Wrapped in a transaction to revert settings back to normal after EXPLAIN
      @conn.transaction do
        settings.each do |k,v|
          @conn.exec("SET %s = %s" % [k, v])
        end
        
        res = @conn.exec("EXPLAIN (FORMAT JSON, VERBOSE TRUE) %s" % query)
        
        @conn.exec "ROLLBACK"
        @conn.exec "BEGIN"
      end
      
      JSON.parse(res[0]["QUERY PLAN"])
    end
    
    def destroy
      @conn.close
      otherconn = PG.connect(@otherdb)
      otherconn.exec_params("DROP DATABASE %s" % otherconn.quote_ident(@dbname))
    end
  end
end