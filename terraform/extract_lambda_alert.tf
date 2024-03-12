resource "aws_cloudwatch_log_metric_filter" "general_error" {
  name           = "LambdaGeneralError"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/extract_sql_data"

  metric_transformation {
    name      = "ErrorCount"
    namespace = "CustomLambdaErrorMetrics"
    value     = "1"
  }
}


resource "aws_cloudwatch_metric_alarm" "general_error_alarm" {
  alarm_name          = "GeneralErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.general_error.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.general_error.metric_transformation[0].namespace
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This metric monitors all errors"
  alarm_actions       = ["arn:aws:sns:eu-west-2:637423384342:test-error-alerts"]
}