Gem::Specification.new do |s|
  s.name        = 'ezq'
  s.version     = '0.2.2'
  s.date        = '2021-08-26'
  s.summary     = "EZQ"
  s.description = "EZQ is e-z"
  s.author      = "Penn Taylor"
  s.email       = 'rpenn3@gmail.com'
  s.platform    = Gem::Platform::RUBY
  s.files       =  Dir.glob("{bin,lib}/**/*")
  s.add_runtime_dependency "aws-sdk", ["2.10.125"]
  s.add_runtime_dependency "deep_merge", ["~>1"]
  s.add_runtime_dependency "rubyzip", ["~>1"]
  s.add_runtime_dependency "parallel", ["~>1"]
  s.add_runtime_dependency "logglier", ["~>0"]
  s.add_runtime_dependency "multi_json", ["~>1"]
  s.add_runtime_dependency "pry", ["~>0"]
  s.add_runtime_dependency "pry-remote", ["~>0"]
  s.add_runtime_dependency "mimemagic", ["~>0.2"]
  s.add_runtime_dependency "sys-filesystem", ["1.1.9"]
  s.license     = 'Private'
  s.homepage    = 'http://stop.warning.me'
  s.executables = ['processor','processor_fan_out','keep_alive','job_breaker','term_processor']
end
