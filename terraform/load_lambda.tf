resource "aws_lambda_function" "load_function" {
  function_name = var.load_lambda_name
  runtime       = "python3.10"
  handler       = "load.lambda_handler"
  role          = aws_iam_role.lambda_load_role.arn
  filename      = "../src/load/load_deployment_package.zip"
  timeout       = 60
}


resource "aws_lambda_permission" "allow_s3" {
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.load_function.function_name
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.processed_data_bucket.arn
    source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "aws-load-lambda-trigger" {
  bucket = aws_s3_bucket.processed_data_bucket.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.load_function.arn
    events              = ["s3:ObjectCreated:*"]
  }
}