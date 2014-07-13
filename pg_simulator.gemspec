$:.push File.expand_path("../lib", __FILE__)
require 'pg_simulator/version'

Gem::Specification.new do |s|
  s.name        = 'pg_simulator'
  s.version     = PgSimulator::VERSION
  
  s.summary     = 'PostgreSQL Schema Simulator'
  s.description = 'Load schema and statistics information into a database and run EXPLAIN on queries'
  s.author      = 'Lukas Fittl'
  s.email       = 'lukas@fittl.com'
  s.license     = 'BSD-3-Clause'
  s.homepage    = 'http://github.com/pganalyze/pg_simulator'

  s.files = %w[
    LICENSE
    Rakefile
    lib/pg_simulator.rb
    lib/pg_simulator/environment.rb
  ]
  
  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec', '~> 2.0'
  
  s.add_runtime_dependency "json", '~> 1.8'
  s.add_runtime_dependency "pg"
end