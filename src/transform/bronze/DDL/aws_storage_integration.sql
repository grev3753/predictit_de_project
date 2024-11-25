create storage integration s3_int
 type = external_stage
 storage_provider = 'S3'
 enabled = true
 storage_aws_role_arn = '${storage_aws_role_arn}'
 storage_allowed_locations = ('${storage_allowed_locations}')
 ;

 DESC INTEGRATION s3_int;