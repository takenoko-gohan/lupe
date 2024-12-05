# Lupe

`lupe` is a CLI tool that enables local log analysis of AWS ALB/S3 access logs.

## Installation

1. Visit the Releases page of this repository.
2. Download the binary corresponding to your operating system (e.g., lupe_X.X.X_linux_amd64.tar.gz for Linux).

## Usage

Create new table

e.g. AWS ALB access logs (default table name is `alb_logs`)
```shell
lupe load --table-type alb --s3-uri 's3://alb-access-logs/AWSLogs/123456789012/elasticloadbalancing/ap-northeast-1/2024/**/*.log.gz'
```

e.g. S3 access logs (default table name is `s3_logs`)

```shell
lupe load --table-type s3 --s3-uri 's3://s3-access-logs/123456789012/ap-northeast-1/alb/2024/11/**/*'
```

Query

```shell
lupe query "SELECT * FROM alb_logs LIMIT 10;"
```

Delete all tables

```shell
lupe clean
```
