require 'securerandom'

module PgSimulator
  class Environment
    # Note: Supplied connection needs to have CREATEDB rights
    def initialize(otherconn, schema_hsh)
      @dbname = "pg_simulator_%s" % SecureRandom.uuid.gsub('-', '_')
      @otherdb = {dbname: otherconn.db, host: otherconn.host, port: otherconn.port, user: otherconn.user}
      
      otherconn.exec_params("CREATE DATABASE %s" % otherconn.quote_ident(@dbname))
      @conn = PG.connect(@otherdb.merge(dbname: @dbname))
      
      create_schema!(schema_hsh)
    rescue => e
      destroy if @conn
      raise
    end
    
    # Schema hash needs to be structured like the one sent by
    # pganalyze-collector
    def create_schema!(schema_hsh)
      schema_hsh.each do |table|
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
        
        @conn.exec_params "UPDATE pg_catalog.pg_class
                              SET relpages = $1, reltuples = $2, relallvisible = $3
                            WHERE oid = $4",
                          [table['stats']['relpages'], table['stats']['reltuples'],
                           table['stats']['relallvisible'], relid]
        
        colstats.each do |position, stats|
          data = {'starelid' => relid, 'staattnum' => position}
          data.merge!(stats)

          keys = []; placeholders = []; values = []
          data.each do |key, value|
            placeholder = "$%d" % (placeholders.size + 1)
            
            if key[/^stanumbers/] && value
              value = value.to_s.gsub('[', '{').gsub(']', '}')
            end
            if key[/^stavalues/]
              placeholder = "array_in(%s, 25, -1)" % placeholder # FIXME: Not everything is text
            end
            
            keys << key
            placeholders << placeholder
            values << value
          end
          
          @conn.exec_params("INSERT INTO pg_catalog.pg_statistic (%s) VALUES (%s)" %
                            [keys.join(","), placeholders.join(",")], values)
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