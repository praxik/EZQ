# @markup markdown
EZQ
===

Synopsis
--------

EZQ provides a simple configuration-style interface to processing messages
in an Amazon SQS queue. The application you use to process messages (your
`*process_command*`) doesn't have to know anything about AWS. If you set up
an AMI containing your `*process_command*`, and set that AMI to run EZQ on
startup, you have instant scalability for a message processing type
application.

EZQ wraps up all the logic of:

* polling and retrieving messages from a queue
* optionally performing a set of pre-processing operations that can be encoded 
  into the messages, such as
  
    * fetching one or more files from Amazon S3
    * fetching the contents of one or more uris
    * decompressing the message body with zlib
  
* executing the `process_command` to process the message (or downloaded file)
* optionally storing job results in Amazon S3, or posting them to a result queue
* optionally posting a "Job complete" message to a result queue

When run on an Amazon EC2 instance, EQZ can also be setup to stop or terminate
the instance when no new messages have arrived in certain period of time.

The full list of configurable options is detailed in 
{file:Processor_Config_Details.md}.

{file:Processor_Message_Details.md} explains in more depth the structure of
messages and how to embed pre-processing directives.

{file:Processor_Example_Use.md} shows an example application using
EZQ::Processor. This shows how "it all fits together."

Simplest Use-Case
-----------------
Once you've setup an appropriate configuration file for EZQ, running a processor
is as simple as

    $ Processor.rb -c my_config_file.yml
    
It's just a matter of getting your configuration file setup correctly....


Getting your feet wet
----------------------

1.  Ensure the aws-sdk gem is installed. If in doubt, issue 
    `gem install aws-sdk`. It may take a few minutes to download and build some 
    parts.
2.  Edit the file `test_programs/config_for_tests.yml`, to fill in your access 
    key id and secret key. Also fill out the s3 bucket name and uri if you want 
    to be able to test those parts. The `queue_name` is set to 'Test_queue'
    by default. You can use a different name if you wish.
3.  Run the test program `add_raw.rb`. This will add 4 raw messages to the sqs 
    queue you named in step one.
4.  Back up into the main EZQ directory and edit `queue_config.yml`. For now, 
    these are the fields you should touch:
    
	  * `access_key_id`
	  * `secret_access_key`
	  * `receive_queue_name`
	  
    Ensure `receive_queue_name` has the same value you gave `queue_name` in 
    step one if you modified that. Now look at the value of `process_command`. 
    This is what will be done to the messages as they are received. Notice that 
    `process_command` could be absolutely anything you can run from the 
    commandline. If you're using MS-Windows, you may need to replace the
    command `cat` with `type`.
5.  Run `processor.rb`. As per the settings in `queue_config.yml`, it will pull 
    messages from your named queue, pass them to 'cat', and then do nothing 
    else.
6.  Use `ctrl-c` to stop `processor.rb`


To test using a result queue:

1.  Go back into `queue_config.yml` and change `result_step` from 'none' to 
    'post_to_result_queue', and enter a queue name in `result_queue_name`. 
    This name should be different from your receive queue. You'll also need to 
    manually create this queue on the SQS web interface. Leave the web 
    interface for the queue open; we'll use it again in a moment.

2.  Run `test_programs/add_raw.rb` again, and then start up `processor.rb`. This 
    time, it will pull messages, cat them, and then post a result message to the 
    `result_queue`. You can select "Refresh" on the right hand side of the SQS 
    web interface to see that messages have been posted to that queue.

3.  Use `ctrl-c` to stop `processor.rb`

Read {file:Processor_Config_Details.md} for a description of all the options
you can change to alter the way EZQ handles messages and results.
