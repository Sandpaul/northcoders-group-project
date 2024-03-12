#This is the actual rule eventbride to trigger the lambda to extract every 5 mins- doesnt have to be
resource "aws_cloudwatch_event_rule" "lambda_trigger_rule" {
  name        = "lambda_extraction_rule"
  description = "Rule to trigger Lambda extraction every 5 minutes"
  schedule_expression = "rate(5 minutes)"
}


#Create an eventbriddge target for the lambda function
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.lambda_trigger_rule.name
  target_id = "lambda_target"

  arn = aws_lambda_function.extract_function.arn
}


# Create an iam role for eventbridge to invoke the lambda
resource "aws_iam_role" "eventbridge_role" {
  name = "eventbridge_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}


#Allows eventbridge to invoke the lambda
resource "aws_lambda_permission" "eventbridge_lambda_permission" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_trigger_rule.arn
}


resource "aws_iam_role_policy_attachment" "example_eventbridge_role_policy_attachment" {
  policy_arn = aws_iam_role_policy_attachment.attach_s3_ingest_policy.policy_arn
  role       = aws_iam_role.eventbridge_role.name
}