resource "aws_iam_role" "lambda_extract_role" {
  name = "lambda_extract_role"

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


resource "aws_iam_policy" "s3_ingest_policy" {
  name        = "s3_ingest_policy"
  description = "Policy for Lambda to ingest data into S3 ingestion bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
        Effect   = "Allow",
        Resource = [
          "arn:aws:s3:::totesys-etl-ingestion-bucket-teamness-120224/*",
          "arn:aws:s3:::totesys-etl-ingestion-bucket-teamness-120224",
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


resource "aws_iam_policy" "extract_lambda_ssm_access" {
  name = "extract_lambda_ssm_access"
  description = "Policy for extract lambda to access ssm"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
        {
            Effect = "Allow",
            Action = ["ssm:GetParameter", "ssm:PutParameter"],
            Resource = "arn:aws:ssm:eu-west-2:637423384342:parameter/last_ingested_timestamp"
        }
    ]
})
}


resource "aws_iam_role_policy_attachment" "attach_s3_ingest_policy" {
  policy_arn = aws_iam_policy.s3_ingest_policy.arn
  role       = aws_iam_role.lambda_extract_role.name
}


resource "aws_iam_role_policy_attachment" "attach_ssm_access_policy" {
  policy_arn = aws_iam_policy.extract_lambda_ssm_access.arn
  role       = aws_iam_role.lambda_extract_role.name
}