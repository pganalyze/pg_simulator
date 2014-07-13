$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = 'pg_simulator'
  s.version     = '0.0.1'
  
  s.summary     = 'PostgreSQL Schema Simulator'
  s.description = 'Load schema and statistics information into a database and run EXPLAIN on queries'
  s.author      = 'Lukas Fittl'
  s.email       = 'lukas@fittl.com'
  s.license     = 'Proprietary'
  s.homepage    = 'http://github.com/lfittl/pg_simulator'

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