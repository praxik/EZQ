require 'aws-sdk'
require_relative './instance'

module Nimbus

class InstanceInfoCollection

  include Enumerable

  def initialize
    @instances
    @client = AWS::EC2::Client.new
  end

  def terminate_all
    @client.terminate_instances({:instance_ids=>ids()})
    return nil
  end

  def stop_all
    @client.stop_instances({:instance_ids=>ids()})
    return nil
  end

  def start_all
    @client.start_instances({:instance_ids=>ids()})
    return nil
  end

  def tag_all(tags = {})
    @client.create_tags({:resources=>ids(),:tags=>tags})
    return nil
  end

  def get_ids
    return @instances.map{|i| i.instance_id}
  end

  alias_method :ids, :get_ids

  # Yields an AWS::EC2::Instance object to the passed block.
  # Use this to perform actions or query attributes for which
  # Nimbus::InstanceInfoCollection does not have a convenience function or
  # Nimbus::Instance does not implement. This way is *much* slower than using
  # Nimbus::InstanceInfoCollection's or Nimbus::Instance's methods.
  def each_instance
    ec2 = AWS::EC2.new
    ids().each{|i| yield(ec2.instances[i])}
    return nil
  end

  def filter(filters)
    @instances = @client.describe_instances({:filters=>filters}).data[:instance_index].map{|i| Instance.new(i[1])}
    return self
  end

  # Direct access
  def [](index)
    return @instances[index]
  end

  # Yields each Nimbus::Instance to the passed block
  def each
    @instances.each{|i| yield(i)}
    return nil
  end

  def size
    return @instances.size()
  end

end #class



end #module
