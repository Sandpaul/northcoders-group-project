resource "aws_iam_role" "lambda_transform_role" {
  name = "lambda_transform_role"

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

resource "aws_iam_policy" "s3_ingest_policy_transform" {
  name        = "s3_ingest_policy_transform"
  description = "Policy for Lambda to get data from ingestion bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:ListBucket"],
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


resource "aws_iam_policy" "s3_process_policy_transform" {
      name        = "s3_processes_policy_transform"
  description = "Policy for Lambda to ingest data into S3 processed bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = ["s3:PutObject", "s3:ListBucket"],
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

resource "aws_iam_role_policy_attachment" "attach_s3_ingest_transform_policy" {
  policy_arn = aws_iam_policy.s3_ingest_policy_transform.arn
  role       = aws_iam_role.lambda_transform_role.name
}

resource "aws_iam_role_policy_attachment" "attach_s3_process_transform_policy" {
  policy_arn = aws_iam_policy.s3_process_policy_transform.arn
  role       = aws_iam_role.lambda_transform_role.name
}