class PgQuery
  # PostgreSQL calls this the range table, we simple name it table references
  attr_reader :table_references

  # This resolves alias => schema table object
  attr_reader :table_aliases

  # This stores the result of rewrite!
  attr_reader :rewritten_parsetree

  # This rewrites the query to reference all tables and columns using their actual objects
  def rewrite!
    raise "Error: Schema proxy not set" unless @schema_proxy

    @table_references = []
    @table_aliases = {}

    @rewritten_parsetree = @parsetree.dup
    statements = @rewritten_parsetree
    from_clause_items = []
    where_clause_items = []

    loop do
      if statement = statements.shift
        case statement.keys[0]
        when "SELECT"
          if statement["SELECT"]["op"] == 0
            (statement["SELECT"]["fromClause"] || []).each do |item|
              if item["RANGESUBSELECT"]
                statements << item["RANGESUBSELECT"]["subquery"]
              else
                from_clause_items << item
              end
            end
          elsif statement["SELECT"]["op"] == 1
            statements << statement["SELECT"]["larg"] if statement["SELECT"]["larg"]
            statements << statement["SELECT"]["rarg"] if statement["SELECT"]["rarg"]
          end
        when "INSERT INTO", "UPDATE", "DELETE FROM", "VACUUM", "COPY", "ALTER TABLE"
          from_clause_items << statement.values[0]["relation"]
        when "EXPLAIN"
          statements << statement["EXPLAIN"]["query"]
        when "CREATE TABLE AS"
          from_clause_items << statement["CREATE TABLE AS"]["into"]["INTOCLAUSE"]["rel"] rescue nil
        when "LOCK"
          from_clause_items += statement["LOCK"]["relations"]
        when "DROP"
          object_type = statement["DROP"]["removeType"]
          if object_type == 26 # Table
            statement["DROP"]["objects"].map! {|rel| @schema_proxy.find_schema_table(*rel.reverse) }
            @table_references += statement["DROP"]["objects"]
          end
        end

        where_clause_items << statement.values[0]["whereClause"] if !statement.empty? && statement.values[0]["whereClause"]
      end

      # Find subselects in WHERE clause
      if next_item = where_clause_items.shift
        case next_item.keys[0]
        when /^AEXPR/, 'ANY'
          ["lexpr", "rexpr"].each do |side|
            next unless elem = next_item.values[0][side]
            if elem.is_a?(Array)
              where_clause_items += elem
            else
              where_clause_items << elem
            end
          end
        when 'SUBLINK'
          statements << next_item["SUBLINK"]["subselect"]
        end
      end

      break if where_clause_items.empty? && statements.empty?
    end

    loop do
      break unless next_item = from_clause_items.shift

      case next_item.keys[0]
      when "JOINEXPR"
        ["larg", "rarg"].each do |side|
          from_clause_items << next_item["JOINEXPR"][side]
        end
      when "ROW"
        from_clause_items += next_item["ROW"]["args"]
      when "RANGEVAR"
        rangevar = next_item["RANGEVAR"]
        table = @schema_proxy.find_schema_table(rangevar["relname"], rangevar["schemaname"])
        @table_references << table
        @table_aliases[rangevar["alias"]["ALIAS"]["aliasname"]] = table if rangevar["alias"]
      end
    end

    @table_references.compact!
    @table_references.uniq!

    # 2. lookup columns in range table
  end
end
