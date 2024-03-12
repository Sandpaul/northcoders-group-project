resource "aws_lambda_function" "transform_function" {
  function_name = var.transform_lambda_name
  runtime       = "python3.10"
  handler       = "lambda_handler.lambda_handler"
  role          = aws_iam_role.lambda_transform_role.arn
  filename      = "../src/transform/transform_deployment_package.zip"
  timeout       = 60
}


resource "aws_lambda_permission" "allow_s3_transform" {
    action = "lambda:InvokeFunction"
    function_name = var.transform_lambda_name
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.ingestion_bucket.arn
    source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "aws-transform-lambda-trigger" {
  bucket = aws_s3_bucket.ingestion_bucket.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_function.arn
    events              = ["s3:ObjectCreated:*"]
  }
}