require 'minitest/autorun'
require_relative '../escaping.rb'


class TestEZQEscaping < Minitest::Test

  def test_unescape
    str = "This\nstring\\must be\tunescaped because it will have 'all sorts' of stuff in it once it is \"dump\"ed."
    assert_equal "\"#{str}\"", EZQ.unescape(str.dump)
  end


  def test_fix_escapes
    str = "\\\ There are extra \\\\ slashes here\n and there!\\n"
    assert_equal "\\\\ There are extra \\\\ slashes here\n and there!\\n", EZQ.fix_escapes(str)
  end

end
