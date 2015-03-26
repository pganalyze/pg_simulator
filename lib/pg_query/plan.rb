class PgQuery
  class Plan
    attr_reader :raw_plan
    def initialize(raw_plan, query)
      @raw_plan = raw_plan
      @query = query
    end

    # Yields every plan node to the block
    def each_node(&block)
      nodes = [@raw_plan]
      loop do
        node = nodes.shift
        nodes += node['Plans'] if node['Plans']

        yield node

        break if nodes.empty?
      end
      nil
    end

    # Deletes the nodes for which the block returns true
    def delete_nodes!(&block)
      # Special case: Root node matches, so the plan is set to nil
      if yield(@raw_plan)
        @raw_plan = nil
        return
      end

      # Delete child nodes matching the expression
      parent_nodes = [@raw_plan]
      loop do
        parent_node = parent_nodes.shift

        if parent_node['Plans']
          parent_node['Plans'].reject! {|node| yield(node) }
          parent_nodes += parent_node['Plans']
        end

        break if parent_nodes.empty?
      end
    end

    # Result: Hash {column => index_name, ...} (index is nil if no index can't be used)
    def indices_per_column
      raise "Error: Schema proxy not set" unless @query.schema_proxy

      result = {}
      each_node do |node|
        # For some plan nodes you can't add an index, ignore them
        next if ['Aggregate'].include?(node['Node Type'])

        condition_to_columns(node, 'Filter').each do |column|
          result[column] ||= []
          result[column] << nil
        end

        condition_to_columns(node, 'Index Cond').each do |column|
          result[column] ||= []
          result[column] << node['Index Name']
        end
      end

      # Avoid duplicates for now
      result.each do |k,v|
        v.uniq!
      end

      result
    end

    # Useful for comparing plans (e.g. in tests), since costs are unpredictable
    def remove_costs!
      each_node do |node|
        node.delete("Startup Cost")
        node.delete("Total Cost")
      end
      nil
    end

  protected
    def condition_to_columns(node, expr_field)
      return [] unless node[expr_field]

      q = format('SELECT 1 FROM dummy WHERE %s', node[expr_field])
      q = PgQuery.parse(q)
      columns = q.filter_columns

      # Resolve aliases to the real table name
      columns.each do |column|
        if column[0] == node['Alias']
          column[0] = node['Relation Name']
        end
      end if node['Alias']

      columns
    end
  end
end
