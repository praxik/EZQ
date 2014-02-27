Installing rusle2_aggregator
============================

1 Install postgres-specific dependencies:
  1 `sudo apt-get install libpq-dev`
  2 `gem install pg`
2 Install EZQ:
  1 Copy over EZQ folder
  2 Run `bundle install`


Running rusle2_aggregator
=========================

rusle2_aggregator is set up as a singleton application in which the first
invocation of the program becomes a server holding a connection to a
database, and subsequent invocations pass data to the server and then
exit. The server instance takes the passed data and stores it in the database.

This setup is designed to allow the aggregator to play nicely as an
EZQ processor application -- where the program is invoked every time a new
message containing results comes in -- but without having to create a new
connection to the database each time.

With this in mind, here's how to run it:

1. Run an instance of rusle2_aggregator with no
   commandline args: `$ ./rusle2_aggregator.rb` This will start the server.
2. Run an EZQ::processor instance with a config set to run
   `./rusle2_aggregator.rb -f $input_file` as the process_command
