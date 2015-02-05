# 1. Create an image from a running instance. For PZM this means creating
#    and image for both the long running instance and the short running instance
#    because each instance has different user data. The two items that need
#    specified by the user or the instance id to image and the name for the
#    AMI that will be created.
aws ec2 create-image --instance-id i-fa3a4d0b --name "worker_pzm_md_production_r10" --block-device-mappings "[{\"DeviceName\": \"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"DeleteOnTermination\":true,\"VolumeType\":\"gp2\"}}]"

aws ec2 create-image --instance-id i-193b4ce8 --name "worker_pzm_md_long_production_r6" --block-device-mappings "[{\"DeviceName\": \"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"DeleteOnTermination\":true,\"VolumeType\":\"gp2\"}}]"

# 2. Create a launch configuration for the new AMI that was just created. This
#    configuration will share the same name as the AMI image created in #1. The
#    additional info needed is the AMI id that was created in #1 and the
#    instance type and price to be used
aws autoscaling create-launch-configuration --launch-configuration-name worker_pzm_md_production_r10 --image-id ami-d45d1fbc --instance-type m1.medium --spot-price "0.034" --security-groups "sg-52f23f37" --key-name "praxik" --block-device-mappings "[{\"DeviceName\": \"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"DeleteOnTermination\":true,\"VolumeType\":\"gp2\"}}]" --instance-monitoring Enabled=False

aws autoscaling create-launch-configuration --launch-configuration-name worker_pzm_md_long_production_r6 --image-id ami-a85d1fc0 --instance-type m1.medium --spot-price "0.034" --security-groups "sg-52f23f37" --key-name "praxik" --block-device-mappings "[{\"DeviceName\": \"/dev/sda1\",\"Ebs\":{\"VolumeSize\":30,\"DeleteOnTermination\":true,\"VolumeType\":\"gp2\"}}]" --instance-monitoring Enabled=False

# 3. Setup the schedules for using the auto scaling group.

# 3.1. This configuration scales instances based on a daily schedule
aws autoscaling put-scheduled-update-group-action --auto-scaling-group-name pzm_md_as_group --scheduled-action-name pzm_daily_worker_startup_schedule --recurrence "0 13 * * MON-FRI" --min-size 1 --max-size 1 --desired-capacity 1

aws autoscaling put-scheduled-update-group-action --auto-scaling-group-name pzm_md_as_group --scheduled-action-name pzm_daily_worker_shutdown_schedule --recurrence "0 1 * * *" --min-size 0 --max-size 1 --desired-capacity 0

# 3.2. Manually configure scaling instances based on sqs cloudwatch alarms
#      through the AWS console UI.
#aws autoscaling put-scheduled-update-group-action --auto-scaling-group-name pzm_md_as_group_sqs --scheduled-action-name pzm_daily_worker_startup_schedule --recurrence "0 13 * * MON-FRI" --min-size 1 --max-size 3 --desired-capacity 1

#aws autoscaling put-scheduled-update-group-action --auto-scaling-group-name pzm_md_as_group_sqs --scheduled-action-name pzm_daily_worker_shutdown_schedule --recurrence "0 1 * * *" --min-size 0 --max-size 3 --desired-capacity 0

# 4. Update the ASG with the new launch group information.

aws autoscaling update-auto-scaling-group --auto-scaling-group-name pzm_md_long_as_group_sqs  --launch-configuration-name "worker_pzm_md_long_production_r6"
aws autoscaling update-auto-scaling-group --auto-scaling-group-name pzm_md_as_group  --launch-configuration-name "worker_pzm_md_production_r10"
aws autoscaling update-auto-scaling-group --auto-scaling-group-name pzm_md_as_group_sqs  --launch-configuration-name "worker_pzm_md_production_r10"

# 5. Look at the info about the ASG
aws autoscaling describe-scheduled-actions --auto-scaling-group-name pzm_md_as_group

aws autoscaling describe-policies --auto-scaling-group-name pzm_md_as_group_sqs
