require 'yaml'

require 'ezq/utils/common'


module EZQ

  # Returns a new string with the **first** EZQ preamble stripped off
  def EZQ.strip_preamble(str)
    return str.sub(/-{3}\nEZQ.+?\.{3}\n/m,'')
  end



  # Replaces the body of an AWS::SQS::RecievedMessage with a version of the
  # body that doesn't contain the **first** EZQ preamble. Returns nil.
  def EZQ.strip_preamble_msg!(msg)
    msg[:body].sub!(/-{3}\nEZQ.+?\.{3}\n/m,'')
    return nil
  end



  # Returns a hash of the EZQ preamble. Does **not** remove the
  # preamble from the message body.
  def EZQ.extract_preamble(msgbody)
    body = YAML.load(msgbody)
    return '' if !body.kind_of?(Hash)
    return '' if !body.has_key?('EZQ')
    return body['EZQ']
  rescue
    return {}
  end

end
