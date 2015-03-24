require 'ezq/utils/common'


module EZQ

  # Returns the local ip of the machine using only standard builtin OS calls.
  # Restricted to Windows and Unix-like OSes.
  def EZQ.get_local_ip()
    ip = ''
    if RUBY_PLATFORM =~ /mswin|mingw/
      cmd = 'FOR /f "tokens=1 delims=:" %d IN (\'ping %computername% -4 -n 1 ^| find /i "reply"\') DO FOR /F "tokens=3 delims= " %g IN ("%d") DO echo %g'
    else
      cmd = 'ifconfig eth0 | sed -n "2s/[^:]*:[ \t]*\([^ ]*\) .*/\1/p"'
    end
    IO.popen(cmd) do |io|
      while !io.eof?
        ip << io.gets
      end
      io.close
    end
    return ip.split().last()
  end

end
