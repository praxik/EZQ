require 'aws-sdk'
require 'securerandom'

module EZQ

  module SNS

    def SNS.publish(topic, message)
      Aws::SNS::Topic.new(topic).publish(message: message)
    end


  class EventMessage
    attr_accessor :message

    def initialize( body = '', id = SecureRandom.uuid )
      @message = Default
      records = @message[ 'Records' ]
      record = records.first
      sns = record[ 'Sns' ]
      sns[ 'Message' ] = body
      sns[ 'MessageId' ] = id
      sns[ 'Timestamp' ] = Time.now.getutc
    end

    def to_json
      @message.to_json
    end

    Default = JSON.parse( %{
      {
        "Records": [
          {
            "EventVersion": "1.0",
            "EventSubscriptionArn": "eventsubscriptionarn",
            "EventSource": "aws:sns",
            "Sns": {
              "SignatureVersion": "1",
              "Timestamp": "1970-01-01T00:00:00.000Z",
              "Signature": "EXAMPLE",
              "SigningCertUrl": "EXAMPLE",
              "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
              "Message": "Hello from SNS!",
              "MessageAttributes": {
                "Test": {
                  "Type": "String",
                  "Value": "TestString"
                },
                "TestBinary": {
                  "Type": "Binary",
                  "Value": "TestBinary"
                }
              },
              "Type": "Notification",
              "UnsubscribeUrl": "EXAMPLE",
              "TopicArn": "topicarn",
              "Subject": "TestInvoke"
            }
          }
        ]
      } } )

  end # class

  end

end
