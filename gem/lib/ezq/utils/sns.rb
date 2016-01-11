require 'aws-sdk'

module EZQ

  module SNS

    def SNS.publish(topic, message)
      Aws::SNS::Topic.new(topic).publish(message: message)
    end

  end

end
