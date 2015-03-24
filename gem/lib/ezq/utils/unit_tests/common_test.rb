require 'minitest/autorun'
require_relative '../common.rb'


class TestEZQCommon < Minitest::Test

  def test_map_slice
    assert_equal [3,7,11,15,19], (1..10).map_slice(2){|a,b| a + b}
  end

  def test_cyclical_fill
    assert_equal [1,2,3,1,2,3,1], EZQ.cyclical_fill([1,2,3],7)
  end

end
