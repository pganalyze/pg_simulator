require 'pg_simulator'

Dir[File.dirname(__FILE__) + '/support/**/*.rb'].each { |f| require f }

RSpec.configure do |config|
  config.include SpecSupport::ExplainQuery
end
