
module Nimbus

class Instance

  def initialize(instance_data)
    @info = instance_data
  end

  def self.define_attribute(name)
    define_method(name) do
      return @info[name]
    end
  end

  define_attribute :product_codes

  define_attribute :block_device_mapping

  define_attribute :tag_set

  define_attribute :group_set

  define_attribute :network_interface_set
  
  define_attribute :instance_id

  alias_method :id, :instance_id

  define_attribute :image_id

  define_attribute :instance_state

  define_attribute :private_dns_name

  define_attribute :dns_name

  define_attribute :reason

  define_attribute :ami_launch_index

  define_attribute :instance_type

  alias_method :type, :instance_type

  define_attribute :launch_time

  define_attribute :placement

  define_attribute :platform

  define_attribute :subnet_id

  define_attribute :vpc_id

  define_attribute :private_ip_address

  define_attribute :source_dest_check

  define_attribute :architecture

  define_attribute :root_device_type

  define_attribute :root_device_name

  define_attribute :virtualization_type

  define_attribute :client_token

  define_attribute :hypervisor

  define_attribute :ebs_optimized

  define_attribute :public_ip_address

  define_attribute :instance_lifecycle

  define_attribute :state_transition_reason

  define_attribute :key_name


  # Some attributes have goofy structure in the hash, so we explicitly convert
  # these into something more useful

  def tags
    # tag_set has form [{:key=>'K1',:value=>'V1'},{:key=>'K2',:value=>'V2'}]
    # We return the more useful {'K1'=>'V1','K2'=>'V2'}
    return Hash[@info[:tag_set].map{|h| h.values}]
  end

  def status
    return @info[:instance_state][:name].to_sym()
  end

  def monitoring_enabled?
    return @info[:monitoring][:state] == 'enabled' ? true : false
  end

  def ebs_optimized?
    return ebs_optimized()
  end

  def spot?
    return instance_lifecycle() == 'spot' ? true : false
  end


end # class

end # module
