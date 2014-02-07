EZQ::Processor Example Use Case
===============================

Say you have a cloud application that lets users submit an image, and your
application rotates, crops, and resizes the image, then stores the transformed
image in Amazon S3. To handle this, you've written small application called
`transform` which does the image transformation. It's called from the
commandline like this:

    transform -t operations_file -i input_image_file -o output_image_file

Transform applies the operations listed in operations_file to input_image_file,
then saves the resulting image as output_image_file.

To handle this simple workflow with EZQ::Processor, you first set up an
SQS queue called Task_Queue. Whenever a user selects a file and clicks "Submit"
on your web front-end, your web app uploads the file to an S3 bucket called
Incoming and then places a message like this into Task_Queue:

    ---
    EZQ:
      get_s3_files:
      - bucket: Incoming
        key: this_users_image

      put_s3_files:
      - bucket: Finished
        key: this_users_transformed_image
        filename: output.jpg
    ...
    rotate,crop,resize

Sometime previously, you've set up an EC2 AMI that has `transform` and
EZQ::Processor installed on it. The queue_config.yaml on this AMI looks like
this:

    access_key_id: YOUR_ACCESS_KEY_ID
    secret_access_key: YOUR_SECRET_ACCESS_KEY
    receive_queue_name: Task_Queue
    store_message: false
    decompress_message: false
    process_command: "transform -t $input_file -i $s3_1 -o output.jpg"
    retry_on_failure: false
    retries: 0
    polling_options:
      :wait_time_seconds: 20
      :attributes:
        - :all
    result_step: none
    result_queue_name: ""
    compress_result_message: false
    keep_trail: false
    cleanup_command: ""
    halt_instance_on_timeout: false
    smart_halt_when_idle_N_seconds: 0
    halt_type: terminate

Additionally, you've set the AMI to run EZQ::Processor on startup.

That's it. Start up one or more instances of this worker AMI, and each will
begin to poll the SQS queue "Task_Queue". When a worker gets the task message,
it will pull down the file this_users_image and save

    rotate,crop,resize

into a file with a unique name -- we'll use 8888888.in as an example.
In the process_command string, EZQ::Processor will replace the reference to
$input_file with 8888888.in, and the reference to $s3_1 with the key of the
first file pulled from s3: in this case, this_users_image. So transform will
get executed as:

    transform -t 8888888.in -i this_users_image -o output.jpg

Once transform exits, EZQ::Processor will pick up the file output.jpg and will
store it in S3 in the bucket "Finished" with key "this_users_transformed_image".
Then EZQ::Processor will begin polling Task_Queue for the next task.

Transform never has to know anything about AWS because it never interacts
directly with SQS, S3, etc. If you later decide to replace `transform` with
a different image manipulation program -- perhaps an existing one such as
ImageMagick -- it's not a huge undertaking: you just change the process_command
in queue_config.yml to accomodate the new program.

