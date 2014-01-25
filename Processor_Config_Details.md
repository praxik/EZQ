Introduction to EZQ::Processor Configuration
============================================

Configuration options for EZQ::Processor can be set in two main ways:

1. Specifying the options in a config.yml file
2. Specifying the options in a Ruby hash

The second method only applies if using the EZQ::Processor directly in
Ruby code; the first can apply in this instance as well as when using
Processor.rb as a standalone application.

Naming a config file **queue_config.yml** will allow Processor.rb to find it
without having to specify a config filename on the commandline.

Any configuration option marked as *Overridable* can be overridden by 
embedding EZQ directives into a message. These directives should be in YAML
format using the key EZQ. For example:

    EZQ:
     store_message: true
     keep_trail: true

Overridden settings only stay overridden for the duration of the message which
overrode them, unless marked as *Overridable:sticky*

If +receive_queue_type+ is set to to "raw", then EZQ directives can be safely 
added by putting a YAML block like this at the *head* of the message:

    ---
    EZQ:
     option1: value1
     option2: value2
    ...

The `---` and `...` mark the block as YAML. EZQ will strip out this entire block
before passing the message on to your chosen processor. If the rest of your
message is also YAML, don't worry: it won't be touched. EZQ only attempts to 
pre-process the first YAML document in the message stream, and only strip it
from the message if it matches this regexp: `/-{3}\nEZQ.+?\.{3}\n/m`

Example Configuration File
==========================
Contents of an example +queue_config.yml+:

    access_key_id: YOUR_ACCESS_KEY_ID
    secret_access_key: YOUR_SECRET_ACCESS_KEY
    receive_queue_name: YOUR_QUEUE_FROM_WHICH_TO_RECEIVE_JOBS
    recieve_queue_type: raw
    store_message: false
    decompress_message: false
    process_command: "cat $input_file"
    retry_on_failure: false
    retries: 0
    polling_options:
      :wait_time_seconds: 20
    result_step: none
    result_queue_name: ""
    result_queue_type: raw
    result_s3_bucket: ""
    compress_result: false
    keep_trail: false
    cleanup_command: ""
    halt_instance_on_timeout: false
    smart_halt_when_idle_N_seconds: 0
    halt_type: stop

Configuration Options
=====================

+access_key_id+ (String)
------------------------
  Your AWS Access Key ID 
  (for more details: http://aws.amazon.com/security-credentials )

+secret_access_key+ (String)
----------------------------
  Your AWS Secret Access Key 
  (for more details: http://aws.amazon.com/security-credentials )

+receive_queue_name+ (String)
---------------------------------
  Name of SQS queue, attached to the account with above +access_key_id+, which
  should be polled for messages to process.
  
+receive_queue_type+ (String,_Overridable_)
-----------------------------------------------
  Queue type. Valid values: 
    * **"raw"** -- After extracting and stripping EZQ directives, the remaining 
      body will be sent directly to +process_command+.
    * **"s3"** -- The message body is formatted as yaml containing two fields: 
      *bucket* and *key*, which refer to the Amazon S3 properties of the same 
      name. The referenced file will be downloaded then passed to 
      +process_command+.
    * **"uri"** -- The message body is formatted as yaml, containing at least 
      one field called 'uri' which contains a normal uri. The referenced file 
      will be downloaded, optionally decompressed by zlib, then passed to 
      +process_command+
      
+store_message+ (String,_Overridable_)
--------------------------------------
  If true, the entire message, with 
  all its meta-information, will be written to the file _$id.message_, where 
  _$id_ is the same as the _$id_ parameter sent to +process_command+. Set this 
  field to true if your queue messages contain information that is unused by 
  EZQ, but which is needed by your processing application, or if you wish 
  access to message meta-information such as time stamps. The meta-information
  stored with messages can be customized by the +:attributes+ variable of 
  +polling_options+. 

+decompress_message+ (String,_Overridable_)
-------------------------------------------
  Should the message or 
  referenced file be decompressed with zlib? (true/false) The message or 
  referenced file *must* have been compressed either directly with zlib, or 
  with gzip. If +receive_queue_type+ is 'raw', the message will be run through 
  Ruby's Base64 decoder before being decompressed. (SQS won't accept binary 
  data into a queue, so you must have encoded it in Base64, right? Right!?)
 
+process_command+ (String,_Overridable_)
----------------------------------------
  Command to run for processing messages. 
  This command should return exit code 0 if the message was processed 
  successfully and should be deleted from the queue. It should return anything
  other than zero if processing failed and the message should not be deleted 
  from the queue. 
  Commands should be specified as a string in double quotes, including any 
  arguments to the process command. 
  Example: 
   
         process_command: "cat $input_file > $id.output; ls -Fal $full_msg_file"
  Three special variables are available to be expanded inside the command 
  string: 
  **$input_file**, **$id**, and **$full_msg_file**. These variables are expanded *before*
  passing the command to the system's shell.
   
  * **$input file** -- The named file contains the contents of the message body 
               (or the contents to the referenced file in the case of s3 or url 
               queue types). 
                       
  * **$id** -- A string containing a unique ID associated with a message. 
               See the example process_command above as well as the 
               documentation for +result_queue_type+ for more information 
               about a specific use of this ID. 
                
  * **$full_msg_file** -- The named file containins the full message. This 
               file only exists if +store_message+ is set to true.
         
+retry_on_failure+ (String,_Overridable_)
-----------------------------------------
  Should +process_command+ be re-run on the *same message* if processing 
  failed? (true/false)
  
+retries+ (Integer,_Overridable_)
---------------------------------
  Number of times we should retry on 
  failure for a single messsage. This field has meaning only if 
  +retry_on_failure+ is true.
  
+polling_options+ (Hash)
------------------------
  Polling options that are passed directly to 
  the call to poll. Use '{}' for no options. Example:
  
          polling_options: 
            :wait_time_seconds: 20
            :batch_size: 2
  **Be sure to use the leading colon on option names.** It's not +batch_size+;
  it's **+:batch_size+**. Indentation is important -- all options should be 
  indented by the same amount below +polling_options+.
  
  If a batch size larger than 1 is used, EZQ will request that
  number of messages at a time, cache the messages, then hand them one by one
  to +process_command+. If +retry_on_failure+ is true, retries will occur on a 
  message before moving to the next message in the cache.
  
  The effect of a non-zero **+:idle_timeout+** is to exit EZQ after waiting the 
  specified number of seconds with no messages received. If you want to poll 
  indefinitely (and you probably do!), do not specify an +idle_timeout+.
  
  See SQS documentation at 
  http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/SQS/Queue.html#poll-instance_method 
  for a full explanation of all available options.
  
+result_step+ (String,_Overridable_)
------------------------------------
  What to do following successful processing of a message. Valid values are:
  
  * **none** -- The processing command itself takes care of anything that 
        needs to be done with results.
  * **post_to_result_queue** -- post a message to the results queue. 
        Like +receive_queue+, this queue has a type which indicates what
        information should be posted. The queue must already exist; EZQ makes
        no attempt to create +result_queue+ if it does not exist.
  
+result_queue_name+ (String,_Overridble_)
-----------------------------------------
  Name of the queue to which to post results. This and all subsequent fields
  only have meaning if +result_step+ is set to _post_to_results_queue_. The queue 
  must already exist; EZQ makes no attempt to create the +result_queue+ if it 
  does not exist.
  
+result_queue_type+ (String,_Overridable_)
------------------------------------------
  The type of result queue. Valid values are:
  
  * **raw** -- The contents of the file `output\_$id.txt` will be optionally 
           compressed via zlib and encoded as base64, then placed directly into a 
           field named 'notes'. 
           If the file `output\_$id.txt` does not exist, then the 'notes' field will
           contain the text 'No output'.
  * **s3** --  The contents of the file `output\_$id.tar` will be optionally 
           compressed via zlib, then uploaded to the Amazon s3 bucket specified 
           in the option +result_s3_bucket+. The bucket name and key will be 
           placed into the 'notes' field as yaml. If `output\_$id.tar` does
           not exist, nothing will be uploaded, and the 'notes' field will 
           contain the text 'No output'. **NB**: The file must have the 
           extension ".tar" even if it is not actually an archive.
  
  In both cases, a message formatted in yaml will be posted to the queue. It
  will contain at least one field: 'processed_message_id', the value of which is
  the id of the message that was processed to produce this result. The other 
  contents of the message will differ as specified above. *$id* refers to the 
  *$id* parameter which was passed to +process_command+.
  
+result_s3_bucket+ (String,_Overridable_)
-----------------------------------------
  The name of the s3 bucket in which to store results. Option is meaningful only
  if +result_queue_type+ is set to 's3'. The bucket should exist; EZQ makes no
  attempt to create the bucket if it doesn't exist.
  
+compress_result+ (String,_Overridable_)
----------------------------------------
  Should the results file be compressed with zlib? (true/false)
  
+keep_trail+ (String,_Overridable_)
-----------------------------------
  If true, EZQ will not delete most of the temporary input or 
  output files created during the course of processing messages. This includes
  *$input_file*, *output_$id.txt* or *output_$id.tar*, and *$id.message* (if 
  +store_message+ was true). Temporary files that are created when compressing or
  decompressing queue messages are always deleted, regardless of the value of 
  this option.
  
+cleanup_command+ (String,_Overridable_)
----------------------------------------
 The command to run to cleanup after processing has succeeded and the message
  has been deleted from the +receive_queue+. The same variables that can be 
  expanded for +process_command+ function here, too. The cleanup command is 
  responsible for deleting any desired temporary files created by 
  +process_command+, as well as all the files generated by EZQ if 
  +keep_trail+ was set to true. Set this option to "" if you don't require 
  special cleanup.
  
+halt_instance_on_timeout+ (String,*Overridable: sticky*)
---------------------------------------------------------
  (true/false) If true, EZQ will stop or terminate -- whichever is set in 
  +halt_type+ -- the EC2 instance running this when queue polling times out. 
  This only has an effect if EZQ is running on an Amazon EC2 instance and if 
  +polling_options+ has a non-zero +:idle_timeout+. You probably don't want to 
  turn this option on. Really. You probably want the option after this one. 
  Just leave this one false unless you're absolutely positive this is what you 
  want.

+smart_halt_when_idle_N_seconds+ (Integer)
------------------------------------------
  Setting this option to a positive, non-zero value causes EZQ to stop or 
  terminate -- whichever is set in +halt_type+ -- the instance if no message has 
  been received in N seconds AND if the uptime of the machine is less than 
  60 seconds from rolling over to the next hour. Halting an instance before 
  rolling over into the next hour helps maximize the compute/cost ratio for an
  instance. 
 
  You'll probably want to set the value fairly high to avoid 
  unneccesarily halting instances when there is just a short lull in 
  activity near an hour boundary. If you're using AWS AutoScale to scale 
  up/down your compute nodes, you will need to consider carefully how this 
  auto-halting might interact with your external scaling rules.
  
  Setting this option to anything greater than zero will turn it on *and* will
  override any +:idle_timeout+ setting specified in +polling_options+. Setting 
  the option to 0 or any negative number will turn it off.
  
+halt_type+ (String,*Overridable: sticky*)
------------------------------------------
  (stop,terminate) Whether to stop or terminate when either 
  +halt_instance_on_timeout+ or +smart_halt_when_idle_N_seconds+ is set.
 
  In most scaling scenarios, terminate will be the appropriate option.
