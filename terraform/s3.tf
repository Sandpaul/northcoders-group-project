resource "aws_s3_bucket" "code_bucket" {
  bucket = "totesys-etl-code-bucket"
}


resource "aws_s3_bucket" "ingestion_bucket" {
  bucket              = "totesys-etl-ingestion-bucket-teamness-120224"
  object_lock_enabled = true
  lifecycle {
    prevent_destroy = true
  }
}


resource "aws_s3_bucket_notification" "ingestion_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id

}


resource "aws_s3_bucket" "processed_data_bucket" {
  bucket              = "totesys-etl-processed-data-bucket-teamness-120224"
  object_lock_enabled = true
  lifecycle {
    prevent_destroy = true
  }
}


 resource "aws_s3_bucket_notification" "processed_data_bucket_notification" {
   bucket = aws_s3_bucket.processed_data_bucket.id
}

