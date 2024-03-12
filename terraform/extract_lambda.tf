resource "aws_lambda_function" "extract_function" {
  function_name = var.extract_lambda_name
  runtime       = "python3.9"
  handler       = "lambda_handler.lambda_handler"
  role          = aws_iam_role.lambda_extract_role.arn
  filename      = "../src/extract/extract_deployment_package.zip"
  depends_on    = [aws_iam_role_policy_attachment.attach_s3_ingest_policy]
  timeout       = 60
}
