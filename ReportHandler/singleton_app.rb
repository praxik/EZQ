require 'socket'

# This class represents a singleton application, an application that only
# allows one copy of itself to be running. This class enforces singleness by
# binding either a named Unix socket or a TCP socket.
#
# To make a singleton application, your application should define a class
# that inherits from SingletonApp and overrides the method +#start_application+.
# If your app needs to exit early -- anywhere that you would normally call
# `exit N` -- you should call `stop_application N`. This will allow SingletonApp
# to gracefully close and clean up its socket.
#
# To run the singleton, create an instance of your derived class and pass
# either a filename to use as a Unix domain socket (Unix platforms only) or an
# IP address and a port number to use for a TCP socket (any platform). Unix
# domain sockets are faster and more secure than TCP sockets, so their use is
# encouraged where available.
#
# Basic Example:
# --------------
#
#     require './singleton_app'
#        
#     class MyApp < SingletonApp
#       def start_application
#         20.times do
#           puts "MyApp is doing something."
#           sleep(1)
#         end
#       end
#     end
#     
#     # Run the application, Unix-only version
#     MyApp.new("/tmp/my_app_socket")
#
#     # Cross-platform version. Notice how the port is specified.
#     MyApp.new('127.0.0.1',:port=>5024)
#
# The basic example allows exactly one instance of the code in
# start_application to run at a time. Subsequent attempts to run MyApp will
# simply exit. If that's all you were looking for, you can skip the rest of
# this documentation.
#
# If you want to know how to pass information from succeeding attempts to
# start your application on to the running singleton, read on.
#
# Passing data to the singleton
# =============================
# **Your SingletonApp-derived class must be created with option**
# `:listen=>true` **to support data passing.**
#
# Data-passing example
# --------------------
# When a second instance of this example is run, it sends the string
# `'Sending data to the singleton'` to the singleton instance. The singleton
# instance reads the string and prints it to STDOUT. If the resulting behavior
# is still unclear, paste this code into a file and comment out the
# appropriate MyApp.new, then run two copies of the app.
#
#     require './singleton_app'
#     
#     class MyApp < SingletonApp
#       def start_application
#         puts "Starting singleton application"
#         20.times do
#           puts "MyApp is doing something."
#           sleep(1)
#           # If we were to meet a condition that would cause us to want to
#           # exit here, we would call
#           #stop_application
#         end
#       end
#     
#       def send_data_to_singleton(connection)
#         puts "Starting client application"
#         connection.puts('Sending data to singleton')
#       end
#     
#       def handle_data_from_client(connection)
#         data = connection.read
#         # Do something with the data...
#         puts "Got: #{data}"
#         # And then return either success or failure
#         return true
#       end
#     end
#    
#     # Run the application, Unix-only version; notice the :listen option
#     MyApp.new("/tmp/my_app_socket",:listen=>true)
#     # Cross-platform version
#     MyApp.new('127.0.0.1',:port=>5024,:listen=>true)
#
# The following table represents two separate terminal sessions running the
# above code from the file `myapp.rb`. Time flows from top to bottom
#
#     |Terminal 1 (First instance)       | Terminal 2 (Second instance)     |
#     |----------------------------------|----------------------------------|
#     | $ ruby myapp.rb                  |                                  |
#     | Starting singleton application   |                                  |
#     | MyApp is doing something.        |                                  |
#     | MyApp is doing something.        | $ ruby myapp.rb                  |
#     | MyApp is doing something.        | Starting client application      |
#     | Got: Sending data to singleton   |                                  |
#     | MyApp is doing something.        | #second instance has exited      |
#     | MyApp is doing something.        |                                  |
#     | MyApp is doing something         |                                  |
#     | ...etc...                        |                                  |
#     |----------------------------------|----------------------------------|
#

class SingletonApp

# Creates and runs a singleton application.
# @param [String] socket_path To use Unix domain sockets (Unix only),
#   socket_path should be a string representing a filename to use as the
#   socket. To use TCP sockets (required on Windows, but also works on Unix),
#   socket_path should be a string representing a numeric IP address, such as
#   '127.0.0.1'.
# @param [Hash] options Options hash. This hash is aware of two named symbols:
#
#  * :port -- specifies the port to use for a TCP socket. Defaults to +nil+. The
#            existence or non-existence of this option is how you specify
#            whether you want a TCP socket or a Unix domain socket.
#  * :listen -- specifies whether the singleton should listen to the socket for 
#              data from clients. Defaults to +false+, which is useful for a
#              simple singleton app that doesn't require data passing. Set this
#              option to true if you need to perform data passing.
#
#
# Examples:
#
#       # Simple singleton app; binds the socket but otherwise ignores it
#       MySingletonApp.new("/tmp/my_socket") # To use Unix domain sockets
#       MySingletonApp.new('127.0.0.1',:port=>1234) # To use TCP sockets
#
#       # Listens on the socket so data passing is possible
#       MySingletonApp.new("/tmp/my_socket",:listen=>true)
#       MySingletonApp.new('127.0.0.1',:port=>1234,:listen=>true)
#
#  When using Unix domain sockets, SingletonApp takes the basic precaution of
#  setting permissions on the socket file so that only the user who invoked the
#  application has read and write permissions (mode 0600). If more (or less)
#  security against malicious attack via the socket is required, it is up to
#  you alter permissions on the socket file in `#start_application` or to
#  place additional security measures into `#send_data_to_singleton`
#  and `#handle_data_from_client`.
def initialize(socket_path,options={})
  defaults = {:port=>nil, :listen=>false}
  options = defaults.merge(options)
  @port = options[:port]
  @listen = options[:listen]
  @tcp = @port != nil ? true : false
  @socket_path = socket_path
  begin
    @listener = @tcp ? TCPServer.new(@socket_path,@port)
                     : UNIXServer.new(@socket_path)
    File.chmod(0600,@socket_path) if !@tcp
    @exited = false
    server = Thread.new{
      loop do
        singleton_conn = @listener.accept
        success = handle_data_from_client(singleton_conn)
        singleton_conn.puts('0')  if success
        singleton_conn.puts('1') if !success
        singleton_conn.close
      end
    } if @listen
    start_application
    server.join if server
    stop_application
  rescue
    if @listen && !@exited
      client_conn = @tcp ? TCPSocket.new(@socket_path,@port)
                         : UNIXSocket.new(@socket_path)
      send_data_to_singleton(client_conn)
      client_conn.close_write
      exit_code = client_conn.read
      client_conn.close
      exit exit_code.to_i
    end
  end
rescue Interrupt
  singleton_app_cleanup
  exit 0
end



private
# Cleans up the socket file on unix platforms.
def singleton_app_cleanup
  File.unlink(@socket_path) if !@tcp && File.exists?(@socket_path)
end



public
# The main entry point to the singleton application's business logic. If you
# have an existing application that you're converting to be a singleton,
# this is where you should place all your normal startup code. You should
# ensure that you call +#stop_application+ when your application is done and
# is ready to terminate; otherwise the application will stay "hung" listening
# for connections on the socket.
def start_application
end



public
# Tells SingletonApp to stop waiting for connections on the socket and to
# release the socket and exit. Use this in your application anywhere you
# would normally call `exit N`. Failure to do so will leave a zombie socket
# that has to be cleaned up manually if using Unix domain sockets. Simply
# `return`-ing from `#start_application` will initiate a clean shutdown as well,
# but sometimes this is overly difficult, such as when your application
# encounters an error deep in a nested stack.
#
# If it's unclear whether your application should be calling this function,
# consider this: `exit` in Ruby is not used for _normal_ termination; it's used
# for _early_ termination. This function obeys the same principle.
# @param [Integer] status The exit status code to use when exiting. The unix
#   tradition is to use 0 for no_error, and 1 or greater to indicate an error.
def stop_application(status=0)
  @exited = true
  @listener.close
  singleton_app_cleanup
  exit status
end



public
# Handles data from clients. This method should
# call `connection.read` or `connection.readline` (or any other
# suitable IO method on `connection`) to get data from a client. It should
# return +true+ if it successfully dealt with the data from the client, and
# false otherwise. The client will take a return value of +true+ to indicate 
# it should provide +0+ as its exit status (no_error). It will take a return
# value of +false+ to mean it should provide +1+ as its exit status (error).
def handle_data_from_client(connection)
  return true
end



public
# Called if a process is not the first copy to be started and
# SingletonApp was created with the option `:listen=>true`. You can use
# `connection.puts` or other IO methods on `connection` to forward data from
# this instance of the process to the singleton process. Your code should not
# close `connection`; SingletonApp handles that for you.
def send_data_to_singleton(connection)
end

end
