require 'ezq/utils/common'


module EZQ

  # Runs an external command.
  #
  # @param [String,Array] cmd The command to run. If +cmd+ is a single
  #   string, it will be run via the default shell. If +cmd+ is an array
  #   with two or more terms, the command will be run without passing
  #   through a shell, with the first array entry referring to the program
  #   name, and all other entries referring to commandline arguments to the
  #   program.
  #
  # @param [Bool] return_output If +false+, does not store all the string
  #   output and simply returns an empty array as the second element in the
  #   return value.
  #
  # @param [block] block Each line of output from the external program is
  #   yielded to the block as it is emitted.
  #
  # @return An array containing a success flag in the first position, and
  #   an array of strings containing all stdout and stderr output in the
  #   second position. The returned success flag can be true, false, or
  #   nil. True means the external command ran and exited with
  #   exit_status = 0. False means the command ran and exited with
  #   exist_status != 0. Nil means an exception was raised in Ruby
  #   when attempting to run the command (command doesn't exist, file
  #   inaccessible, etc., NOT exceptions raised from WITHIN the called
  #   application). In this case, the output array in the second position
  #   will contain the text of the exception.
  #
  def EZQ.exec_cmd(cmd,return_output: true,&block)
    success = false
    output = []
    begin
      IO.popen(cmd,:err=>[:child, :out]) do |io|
        while !io.eof?
          current = io.gets
          output << current if return_output
          yield current if block_given?
        end
        io.close
        success =  $?.to_i.zero?
      end
    rescue => e
      success = nil # mimic behavior of Kernel#system
      output << e
    end
    return [success,output]
  end

end
