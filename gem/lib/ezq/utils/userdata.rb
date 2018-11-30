require 'base64'

module EZQ
  module UserData
    def self.load
      userdata = nil

      if ENV.key?('USERDATA')
        if ENV.key?('USERDATA_ENCODING') && ENV['USERDATA_ENCODING'] == 'base64'
          # userdata is Base64-encoded
          userdata = YAML.load(Base64.decode64(ENV['USERDATA']))
        else
          # assume USERDATA contains plain text
          userdata = YAML.load(ENV['USERDATA'])
        end
      else
        search_paths = [
          '/tmp/userdata.yml',
          File.join(ENV.fetch('LAMBDA_TASK_ROOT','/var/task'),'userdata.yml'),
          File.join(__dir__, 'userdata.yml'),
          File.join(Dir.pwd, 'userdata.yml')
        ]

        search_paths.each do |path|
          begin
            File.open(path, File::RDONLY) do |f|
              userdata = YAML.load(f.read)
            end

            break
          rescue Errno::ENOENT
            next
          end
        end
      end

      raise RuntimeError, 'No userdata found' if userdata.nil?

      # Lift a subset of userdata into separate environment variables
      userdata["ENV"].each{ |k, v| ENV[k.to_s] = v.to_s }

      userdata
    end
  end
end
