resource "aws_iam_role" "lambda_load_role" {
  name = "lambda_load_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "s3_process_policy_load" {
      name        = "s3_process_policy_load"
  description = "Policy for Lambda to get data from processed bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:ListBucket"],
        Effect   = "Allow",
        Resource = [
          "arn:aws:s3:::totesys-etl-processed-data-bucket-teamness-120224/*",
          "arn:aws:s3:::totesys-etl-processed-data-bucket-teamness-120224",
        ],
      },
      {
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Effect   = "Allow",
        Resource = "*",
      },
    ]
  })
}


resource "aws_iam_role_policy_attachment" "attach_s3_process_policy_load" {
  policy_arn = aws_iam_policy.s3_process_policy_load.arn
  role       = aws_iam_role.lambda_load_role.name
}