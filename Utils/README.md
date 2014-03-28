6k Job and Instance Management Tools
====================================

Launching / Starting / Stopping / Terminating Instances
-------------------------------------------------------

Use the 6k script. From a Unix-ish shell, issuing <br>
`$ ./6k` 
<br>will give you
detailed help info for the 6k tool.

Example use:

*  Get help on a particular command: <br>
   `$ ./6k help launch`
   
*  Launch 5 worker instances with 44 processes each: <br>
   `$ ./6k launch worker --count 5 --processes 44 --size c3.8xlarge > worker_ids.txt`  <br>
   The shell redirection is optional, but is useful since it saves the ids of 
   the started instances to a file which can later be passed to terminate.
   
*  Terminate the instances started by the previous example: <br>
   `$ ./6k terminate --idfile worker_ids.txt`
   
*  Stop an instance with a particular (private) ip address: <br>
   `$ ./6k stop --ips 10.1.2.333`
   
*  List all the 6k-associated instances: <br>
   `$ ./6k list all`
   
*  List only the 6k workers: <br>
   `$ ./6k list worker`
   

Start a Job
-----------

To put a message in the 6k_job_test_44 queue, run:

`$ ./start_job.rb`

This will place a message with the text "Start" into that queue, which should
cause Pre-Grid to run.


Clear a Queue
-------------

After a failed job during testing, sometimes it's necessary to delete a bunch
of messages from a queue. Just do:

`$ ./clear_queue.rb Queue_Name_Here`