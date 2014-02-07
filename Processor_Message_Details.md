EZQ::Processor Message Details
==============================

Simple Messages -- Just a body
------------------------------

The simplest task message you might put into a queue to process would be plain
text, for example:

    Hello World!

EZQ::Processor would download this text from the queue, save it to a file, and
then call your `process_command`. Your `process_command` can read the file and
do whatever it needs with the message text -- print it to the screen, for
example.

A slightly more complicated task message might be information structured in
JSON, like this:

    {
      "JobID" : "77ab-2345-ddcc-4444",
      "Key 1" : "Value 1",
      "Key 2" : "Your process_command will do something with this information."
    }

Both of these messages are examples of a message _body_ and no more. Simple
applications can get by with nothing more than a message body.

Using Directives to Handle Files
--------------------------------

EZQ::Processor can do more than just save a message to a file and execute
your `process_command`. Before calling your `process_command`, it can download
files from Amazon S3 and save the contents of uris into files. After your
process_command finishes, EZQ::Processor can upload result files to an
S3 bucket of your choosing. It can also post a message to a result queue
giving some information about what it has done.

In order to accomplish such behaviors, a _preamble_ containing directives
is added to the message *before* the body.

Consider this task message:

    ---
    EZQ:
      get_s3_files:
      - bucket: Some_S3_Bucket
        key: some_object_in_the_bucket
      - bucket: Some_S3_Bucket
        key: some_other_object_in_the_bucket
      - bucket: A_Different_S3_Bucket
        key: an_object_in_this_bucket

      get_uri_contents:
      - uri: http://example.com/subdir/test.html
        save_name: local_file.html

      put_s3_files:
      - bucket: Result_Bucket
        key: foo.bar
        filename: some_local_result_file.baz
    ...
    {
      "JobID" : "77ab-2345-ddcc-4444",
      "Key 1" : "Value 1",
      "Key 2" : "Your process_command will do something with this information."
    }

The _preamble_ is everything between `---` and `...`, and the _body_ is exactly
the same as the second simple body shown previsouly.
When EZQ::Processor receives this message, it will take the following steps:

1. Download each of the files mentioned under `get_s3_files`, saving each file
   to local storage using the *key* as the filename
2. Download the contents of `http://example.com/subdir/test.html` and save those
   contents into `local_file.html`
3. Strip the preamble containing EZQ directives out of the message and save the
   remaining body as `$input_file`
4. Execute your `process_command`
5. Look in the working directory for the file `some_local_result_file.baz`. If
   found, upload this file into `Result_Bucket` using the key `foo.bar`
6. If `result_step` is set to `post_to_result_queue`, read in the contents of
   the local file `output_$id.txt` and post a message to the result queue
   specified in the configuration setting `result_queue_name`. The result
   message will look like:

<pre>
  ---
  EZQ:
    processed_message_id: 34783475894358437854
    get_s3_files:
    - bucket: Result_Bucket
      key: foo.bar
  ...
  The contents of output_$id.txt would appear here.
</pre>

The value of `processed_message_id` is the id assigned to the originating task
message by Amazon SQS.

Notice that the bucket and key of the result file from the task message --
Result_Bucket/foo.bar -- has been placed into a _get_s3_files_ directive. Aside
from merely letting you know where the results were placed, this format allows
processes to be chained; that is, this result message can be used directly as
a task message for another process sitting in the next stage. If you're a Unix
user, you can think of this as being much like piping results between commands.

If you need to add other directives into a result message, you simply embed an
appropriate preamble into the contents of output_$id.txt. EZQ::Processor will
see that preamble, and add its own structured information into it (eg.
the processed_message_id and a get_s3_files directive corresponding to the
put_s3_files directive in the originating task message).

Using Directives to Override Settings
-------------------------------------

Directives can also be used to override many of the settings that are normally
set at startup by reading a configuration file. The following message, for
example, tells EZQ::Processor to post the results of this *one message* to
the queue "Alternate_Result_Queue" rather than to whichever result queue was set
at configuration time.
  
    ---
    EZQ:
      result_queue_name: Alternate_Result_Queue
    ...
    The message body.

Settings that can be overridden in this manner are maked with the flag
_Overridable_ in {file:Processor_Config_Details.md}.
