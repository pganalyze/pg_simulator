require 'pg'
require 'pg_simulator'

class PgQuery
  attr_writer :simulator_connection
end
