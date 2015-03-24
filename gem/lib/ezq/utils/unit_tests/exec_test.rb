require 'minitest/autorun'
require_relative '../exec.rb'


class TestEZQExec < Minitest::Test

  def test_success
    success, output = EZQ.exec_cmd('ruby cmd_test.rb success')
    assert_equal true, success
    assert_equal ["hello\n","world\n"], output
  end

  def test_success_noshell
    success, output = EZQ.exec_cmd(['ruby', 'cmd_test.rb', 'success'])
    assert_equal true, success
    assert_equal ["hello\n","world\n"], output
  end

  def test_fail
    success, output = EZQ.exec_cmd('ruby cmd_test.rb failure')
    assert_equal false, success
    assert_equal ["hello\n","world\n"], output
  end

  def test_does_not_exist
    success, output = EZQ.exec_cmd('program_that_does_not_exist')
    assert_equal nil, success
  end

  def test_program_exception
    success, output = EZQ.exec_cmd('ruby cmd_test.rb fault')
    # Notice we test for false, not nil. Since the exception is
    # raised from within the program, it returns a failure status.
    # You can see this behavior on a *nix command line by issuing
    # ruby cmd_test.rb fault; echo $?
    assert_equal false, success
    # Test that the exception text occurs *somewhere* in the output.
    assert output.reduce(false){|acc,str| acc or str =~ /An exception! \(RuntimeError\)/}
  end

  def test_matchers
    hellos = 0
    worlds = 0
    success, output = EZQ.exec_cmd('ruby cmd_test.rb success') do |str|
      if str =~ /hello/
        hellos += 1
      elsif str =~ /world/
        worlds += 1
      end
    end
    assert_equal 1, hellos
    assert_equal 1, worlds
  end

end
