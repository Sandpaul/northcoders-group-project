variable "extract_lambda_name" {
  type    = string
  default = "extract_sql_data"
}


variable "transform_lambda_name" {
  type    = string
  default = "transform_sql_data"
}


variable "load_lambda_name" {
  type    = string
  default = "load_sql_data"
}