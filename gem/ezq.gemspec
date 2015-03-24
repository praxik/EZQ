Gem::Specification.new do |s|
  s.name        = 'ezq'
  s.version     = '0.1.0'
  s.date        = '2015-03-20'
  s.summary     = "EZQ"
  s.description = "EZQ"
  s.author      = "Penn Taylor"
  s.email       = 'rpenn3@gmail.com'
  s.files       =  Dir.glob("{bin,lib}/**/*")
  s.add_runtime_dependency "aws-sdk", ["< 2.0"]
  s.add_runtime_dependency "deep_merge"
  s.add_runtime_dependency "rubyzip"
  s.add_runtime_dependency "parallel"
  s.add_runtime_dependency "logglier"
  s.add_runtime_dependency "multi_json"
  s.add_runtime_dependency "pry"
  s.add_runtime_dependency "pry-remote"
  s.add_runtime_dependency "mimemagic"
  s.license     = 'Private'
  s.executables = ['processor','processor_fan_out','keep_alive','job_breaker','term_processor']
end
